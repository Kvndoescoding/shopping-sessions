// Shopping Sessions: native pipeline.
// Reads /data CSVs, writes /output/{user_daily,campaign_daily}/*.parquet.
// Rules: rules.md. Design: parallel chunked CSV parse -> hash-partitioned
// dedup on event_id -> hash-partitioned by person -> sort, sessionize,
// attribute -> minimal Parquet writer (PLAIN, uncompressed).
#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <functional>
#include <string>
#include <thread>
#include <vector>
#ifdef __AVX2__
#include <immintrin.h>
#endif
#ifdef _WIN32
extern "C" __declspec(dllimport) int __stdcall SetProcessAffinityMask(void*, unsigned long long);
extern "C" __declspec(dllimport) void* __stdcall GetCurrentProcess(void);
#endif
#ifndef _WIN32
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

using namespace std;
namespace fs = std::filesystem;
typedef int64_t i64;
typedef uint64_t u64;
typedef uint32_t u32;
typedef int32_t i32;
typedef uint16_t u16;
typedef uint8_t u8;

static const i64 DAY = 86400000, GAP = 1800000, WEEK = 7 * DAY;
static const i64 BAD = INT64_MIN;  // bad timestamp
static const i64 FAR = INT64_MAX;  // valid number, outside any window

static bool PROF = false;
static int EXP = 0;
static int STOP = 0;  // parse experiments: stop after a stage
static thread_local u64 nl_count = 0;
static auto T0 = chrono::steady_clock::now();
static void prof(const char* what) {
    if (!PROF) return;
    double s = chrono::duration<double>(chrono::steady_clock::now() - T0).count();
    fprintf(stderr, "%7.3f  %s\n", s, what);
}

// ------------------------------------------------------------------ hashing
static inline u64 mum(u64 a, u64 b) {
    __uint128_t r = (__uint128_t)a * b;
    return (u64)r ^ (u64)(r >> 64);
}
static inline u64 rd8(const char* p) { u64 v; memcpy(&v, p, 8); return v; }
static inline u64 rd4(const char* p) { u32 v; memcpy(&v, p, 4); return v; }
static inline u64 hstr(const char* p, u32 n) {
    const u64 s0 = 0xa0761d6478bd642full, s1 = 0xe7037ed1a0b428dbull, s2 = 0x8ebc6af09c88c6e3ull;
    u64 a, b, seed = s0 ^ ((u64)n * s2);
    if (n <= 16) {
        if (n >= 4) {
            a = (rd4(p) << 32) | rd4(p + ((n >> 3) << 2));
            b = (rd4(p + n - 4) << 32) | rd4(p + n - 4 - ((n >> 3) << 2));
        } else if (n > 0) {
            a = ((u64)(u8)p[0] << 16) | ((u64)(u8)p[n >> 1] << 8) | (u8)p[n - 1];
            b = 0;
        } else a = b = 0;
    } else {
        u32 i = n;
        const char* q = p;
        while (i > 16) {
            seed = mum(rd8(q) ^ s1, rd8(q + 8) ^ seed);
            q += 16;
            i -= 16;
        }
        a = rd8(p + n - 16);
        b = rd8(p + n - 8);
    }
    return mum(s1 ^ (u64)n, mum(a ^ s1, b ^ seed));
}

// ------------------------------------------------------------------ threads
static int NT = 8;
static thread_local int TL_NT = 0;  // per-thread override of NT (1 = run inline)
static void parallel_for(size_t n, const function<void(size_t, int)>& fn) {
    if (TL_NT == 1) {
        for (size_t i = 0; i < n; i++) fn(i, 0);
        return;
    }
    atomic<size_t> next(0);
    vector<thread> ts;
    vector<double> busy(NT, 0), done(NT, 0);
    auto w0 = chrono::steady_clock::now();
    for (int t = 0; t < NT; t++)
        ts.emplace_back([&, t] {
            for (;;) {
                size_t i = next.fetch_add(1);
                if (i >= n) break;
                auto a = chrono::steady_clock::now();
                fn(i, t);
                busy[t] += chrono::duration<double>(chrono::steady_clock::now() - a).count();
            }
            done[t] = chrono::duration<double>(chrono::steady_clock::now() - w0).count();
        });
    for (auto& x : ts) x.join();
    if (PROF) {
        double wall = chrono::duration<double>(chrono::steady_clock::now() - w0).count();
        double mn = 1e9, mx = 0, sum = 0, fmn = 1e9, fmx = 0;
        for (int t = 0; t < NT; t++) {
            mn = min(mn, busy[t]); mx = max(mx, busy[t]); sum += busy[t];
            fmn = min(fmn, done[t]); fmx = max(fmx, done[t]);
        }
        fprintf(stderr, "    pf n=%zu wall %.3f  busy avg %.3f min %.3f max %.3f  first-done %.3f last-done %.3f\n", n, wall, sum / NT, mn, mx, fmn, fmx);
    }
}

// --------------------------------------------------------------------- files
struct File {
    const char* data = nullptr;
    size_t size = 0;
};
static File load(const string& path) {
    File f;
#ifndef _WIN32
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); exit(2); }
    struct stat st;
    fstat(fd, &st);
    f.size = st.st_size;
    if (f.size) {
        void* m = mmap(nullptr, f.size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (m == MAP_FAILED) { perror("mmap"); exit(2); }
        madvise(m, f.size, MADV_WILLNEED);  // start readahead; parse threads fault pages in parallel
        f.data = (const char*)m;
    }
    close(fd);
#else
    FILE* fp = fopen(path.c_str(), "rb");
    if (!fp) { perror(path.c_str()); exit(2); }
    fseek(fp, 0, SEEK_END);
    f.size = ftell(fp);
    fseek(fp, 0, SEEK_SET);
    char* b = (char*)malloc(f.size + 1);
    if (f.size && fread(b, 1, f.size, fp) != f.size) { perror("read"); exit(2); }
    fclose(fp);
    f.data = b;
#endif
    return f;
}
static vector<string> list_csv(const string& dir) {
    vector<string> r;
    error_code ec;
    for (auto& e : fs::directory_iterator(dir, ec)) {
        if (!e.is_regular_file()) continue;
        if (e.path().extension() == ".csv") r.push_back(e.path().string());
    }
    sort(r.begin(), r.end());
    return r;
}

// ----------------------------------------------------------------------- csv
struct View {
    const char* p;
    u32 n;
};
// Split [s, le) into exactly nf fields; false if malformed.
static inline bool split(const char* s, const char* le, int nf, View* f) {
    if (s == le) return false;
    int k = 0;
    for (;;) {
        const char* c = s;
        int q = 0;
        while (c < le) {
            char ch = *c;
            if (ch == ',') break;
            q += (ch == '"');
            c++;
        }
        if (k >= nf) return false;
        if (q == 0) {
            f[k].p = s;
            f[k].n = (u32)(c - s);
        } else if (q == 2 && c - s >= 2 && *s == '"' && c[-1] == '"') {
            f[k].p = s + 1;
            f[k].n = (u32)(c - s - 2);
        } else return false;
        k++;
        if (c == le) break;
        s = c + 1;
    }
    return k == nf;
}
// Iterate the lines whose first byte lies in [s, e) of file f (skipping the header).
template <class F>
static void for_lines(const File& f, size_t s, size_t e, F&& fn) {
    const char* base = f.data;
    const char* end = base + f.size;
    const char* p;
    if (s == 0) {
        const char* nl = (const char*)memchr(base, '\n', f.size);
        if (!nl) return;
        p = nl + 1;
    } else if (base[s - 1] == '\n') p = base + s;
    else {
        const char* nl = (const char*)memchr(base + s, '\n', end - (base + s));
        if (!nl) return;
        p = nl + 1;
    }
    const char* lim = base + e;
    while (p < lim) {
        const char* nl = (const char*)memchr(p, '\n', end - p);
        const char* le = nl ? nl : end;
        fn(p, le);
        p = nl ? nl + 1 : end;
    }
}
// Like for_lines, but splits each line into exactly NF fields and calls
// fn(View*) only for well-formed lines. AVX2 finds commas, quotes and newlines
// 64 bytes at a time; near the end of the buffer it falls back to split().
static inline u64 bit_range(u32 a, u32 b) {  // bits [a, b), 0 <= a <= b <= 64
    u64 hi = b >= 64 ? ~0ull : ((1ull << b) - 1);
    u64 lo = a >= 64 ? ~0ull : ((1ull << a) - 1);
    return hi & ~lo;
}
template <int NF, class F>
static void for_fields(const File& f, size_t s, size_t e, F&& fn) {
    // One streaming pass: every 64-byte block is classified once (commas,
    // quotes, newlines), and field / line state carries across blocks.
    const char* base = f.data;
    const char* end = base + f.size;
    const char* p;
    if (s == 0) {
        const char* nl = (const char*)memchr(base, '\n', f.size);
        if (!nl) return;
        p = nl + 1;
    } else if (base[s - 1] == '\n') p = base + s;
    else {
        const char* nl = (const char*)memchr(base + s, '\n', end - (base + s));
        if (!nl) return;
        p = nl + 1;
    }
    const char* lim = base + e;
    if (p >= lim || p >= end) return;
    View fl[NF];
    int k = 0;
    u32 qc = 0;
    bool bad = false;
    const char* fs = p;  // current field start
    auto emit = [&](const char* a, const char* b, u32 fq) {
        if (k >= NF) { bad = true; return; }
        u32 len = (u32)(b - a);
        if (fq == 0) fl[k] = View{a, len};
        else if (fq == 2 && len >= 2 && a[0] == '"' && b[-1] == '"') fl[k] = View{a + 1, len - 2};
        else bad = true;
        k++;
    };
#ifdef __AVX2__
    const __m256i vc = _mm256_set1_epi8(','), vq = _mm256_set1_epi8('"'), vn = _mm256_set1_epi8('\n');
    alignas(64) char tail[64];
    const char* w = p;
    for (;;) {
        const char* src = w;
        u64 valid = ~0ull;
        bool last = false;
        if (end - w < 64) {
            size_t r = (size_t)(end - w);
            memcpy(tail, w, r);
            memset(tail + r, 0, 64 - r);
            src = tail;
            valid = bit_range(0, (u32)r);
            last = true;
        }
        __m256i x0 = _mm256_loadu_si256((const __m256i*)src), x1 = _mm256_loadu_si256((const __m256i*)(src + 32));
        u64 C = (u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(x0, vc)) |
                ((u64)(u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(x1, vc)) << 32);
        u64 Q = (u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(x0, vq)) |
                ((u64)(u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(x1, vq)) << 32);
        u64 N = (u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(x0, vn)) |
                ((u64)(u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(x1, vn)) << 32);
        C &= valid;
        Q &= valid;
        N &= valid;
        u64 S = C | N;
        u32 sb = fs > w ? (u32)(fs - w) : 0;
        while (S) {
            u32 idx = (u32)__builtin_ctzll(S);
            S &= S - 1;
            emit(fs, w + idx, qc + (u32)__builtin_popcountll(Q & bit_range(sb, idx)));
            fs = w + idx + 1;
            sb = idx + 1;
            qc = 0;
            if ((N >> idx) & 1) {
                if (!bad && k == NF) fn(fl);
                k = 0;
                bad = false;
                if (fs >= lim) return;  // the next line belongs to the next chunk
            }
        }
        qc += (u32)__builtin_popcountll(Q & bit_range(sb, 64));
        if (last || w + 64 >= end) {
            // end of file: a last line without a trailing newline
            if (fs < end) {
                const char* a = fs;
                // count quotes of the unfinished field directly (simple, runs once per file)
                u32 fq = 0;
                for (const char* c = a; c < end; c++) fq += (*c == '"');
                emit(a, end, fq);
                if (!bad && k == NF) fn(fl);
            }
            return;
        }
        w += 64;
    }
#else
    while (p < lim) {
        const char* nl = (const char*)memchr(p, '\n', end - p);
        const char* le = nl ? nl : end;
        if (split(p, le, NF, fl)) fn(fl);
        p = nl ? nl + 1 : end;
    }
    (void)k; (void)qc; (void)bad; (void)fs; (void)emit;
#endif
}
struct Chunk {
    int file;
    size_t s, e;
};
static void make_chunks(const vector<File>& fs_, vector<Chunk>& out, size_t csz) {
    for (int i = 0; i < (int)fs_.size(); i++)
        for (size_t s = 0; s < fs_[i].size; s += csz) out.push_back({i, s, min(fs_[i].size, s + csz)});
}

// ---------------------------------------------------------------- timestamps
static inline i64 days_from_civil(i64 y, unsigned m, unsigned d) {
    y -= m <= 2;
    const i64 era = (y >= 0 ? y : y - 399) / 400;
    const unsigned yoe = (unsigned)(y - era * 400);
    const unsigned doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
    const unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + (i64)doe - 719468;
}
static inline bool dg(char c) { return (unsigned)(c - '0') < 10; }
static inline int d2(const char* p) { return (p[0] - '0') * 10 + (p[1] - '0'); }
// Days since epoch for every (year, month, day) in the 4 years around the
// window, or INT32_MIN for impossible dates; other years take the slow path.
static int g_y0 = -100000;
static i32 g_dtab[4 * 12 * 32];
static void init_dates(int y0) {
    g_y0 = y0;
    static const int dim[13] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    for (int yi = 0; yi < 4; yi++)
        for (int m = 1; m <= 12; m++)
            for (int d = 0; d < 32; d++) {
                int y = y0 + yi, md = dim[m];
                if (m == 2 && ((y % 4 == 0 && y % 100 != 0) || y % 400 == 0)) md = 29;
                g_dtab[(yi * 12 + m - 1) * 32 + d] = (d >= 1 && d <= md) ? (i32)days_from_civil(y, m, d) : INT32_MIN;
            }
}
static inline i64 parse_ts(const char* p, u32 n) {
    if (n == 0) return BAD;
    if (dg(p[0]) && (n < 5 || dg(p[4]))) {  // maybe F3
        bool all = true;
        for (u32 i = 0; i < n; i++)
            if (!dg(p[i])) { all = false; break; }
        if (all) {
            u64 v = 0;
            for (u32 i = 0; i < n; i++) {
                u64 d = (u64)(p[i] - '0');
                if (v > ((u64)INT64_MAX - d) / 10) return FAR;  // too big: outside every window
                v = v * 10 + d;
            }
            return (i64)v;
        }
    }
    if (n != 20 && n != 25) return BAD;
#ifdef __AVX2__
    {
        // "YYYY-MM-DDTHH:MM" checked in one go: digits where digits belong, separators elsewhere
        const __m128i v = _mm_loadu_si128((const __m128i*)p);  // n >= 20, so 16 bytes are in the field
        const __m128i d = _mm_sub_epi8(v, _mm_set1_epi8('0'));
        const int dm = _mm_movemask_epi8(_mm_cmpeq_epi8(_mm_min_epu8(d, _mm_set1_epi8(9)), d));
        const __m128i tmpl = _mm_setr_epi8('0', '0', '0', '0', '-', '0', '0', '-', '0', '0', 'T', '0', '0', ':', '0', '0');
        const int em = _mm_movemask_epi8(_mm_cmpeq_epi8(v, tmpl));
        const int DIG = 0xDB6F;  // bytes 0-3, 5-6, 8-9, 11-12, 14-15
        const int SEP = 0x2490;  // bytes 4, 7, 10, 13
        if ((dm & DIG) != DIG || (em & SEP) != SEP || p[16] != ':' || !dg(p[17]) || !dg(p[18])) return BAD;
    }
#else
    if (!(dg(p[0]) && dg(p[1]) && dg(p[2]) && dg(p[3]) && p[4] == '-' && dg(p[5]) && dg(p[6]) && p[7] == '-' &&
          dg(p[8]) && dg(p[9]) && p[10] == 'T' && dg(p[11]) && dg(p[12]) && p[13] == ':' && dg(p[14]) &&
          dg(p[15]) && p[16] == ':' && dg(p[17]) && dg(p[18])))
        return BAD;
#endif
    i64 off = 0;
    if (n == 20) {
        if (p[19] != 'Z') return BAD;
    } else {
        char sg = p[19];
        if ((sg != '+' && sg != '-') || !dg(p[20]) || !dg(p[21]) || p[22] != ':' || !dg(p[23]) || !dg(p[24]))
            return BAD;
        int oh = d2(p + 20), om = d2(p + 23);
        if (oh > 23 || om > 59) return BAD;
        off = (i64)(oh * 60 + om) * 60000;
        if (sg == '-') off = -off;
    }
    int y = d2(p) * 100 + d2(p + 2), mo = d2(p + 5), d = d2(p + 8), h = d2(p + 11), mi = d2(p + 14), s = d2(p + 17);
    if (h > 23 || mi > 59 || s > 59) return BAD;
    const unsigned yi = (unsigned)(y - g_y0);
    if (yi < 4 && (unsigned)(mo - 1) < 12) {  // d is 0..99 here; >= 32 is never a valid day
        if (d >= 32) return BAD;
        const i32 dd = g_dtab[(yi * 12 + mo - 1) * 32 + d];
        if (dd == INT32_MIN) return BAD;
        return (((i64)dd * 24 + h) * 60 + mi) * 60000 + (i64)s * 1000 - off;
    }
    if (mo < 1 || mo > 12 || d < 1) return BAD;
    static const int dim[13] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    int md = dim[mo];
    if (mo == 2 && ((y % 4 == 0 && y % 100 != 0) || y % 400 == 0)) md = 29;
    if (d > md) return BAD;
    i64 days = days_from_civil(y, mo, d);
    return ((days * 24 + h) * 60 + mi) * 60000 + (i64)s * 1000 - off;
}
static inline i64 fdiv(i64 a, i64 b) { return a / b - ((a % b != 0) && ((a < 0) != (b < 0))); }

// ------------------------------------------------------------ string tables
struct StrMap {  // small, verified: products, campaigns, categories
    struct E {
        u64 h;
        const char* p;
        u32 n;
        i32 v;
    };
    vector<E> t;
    u64 mask = 0;
    void init(size_t n) {
        size_t c = 16;
        while (c < n * 2) c <<= 1;
        t.assign(c, E{0, nullptr, 0, -1});
        mask = c - 1;
    }
    i32 get(const char* p, u32 n) const {
        u64 h = hstr(p, n);
        for (u64 i = h & mask;; i = (i + 1) & mask) {
            const E& e = t[i];
            if (e.v < 0) return -1;
            if (e.h == h && e.n == n && memcmp(e.p, p, n) == 0) return e.v;
        }
    }
    i32 put(const char* p, u32 n, i32 v) {  // returns existing value or inserts v
        u64 h = hstr(p, n);
        for (u64 i = h & mask;; i = (i + 1) & mask) {
            E& e = t[i];
            if (e.v < 0) { e = E{h, p, n, v}; return v; }
            if (e.h == h && e.n == n && memcmp(e.p, p, n) == 0) return e.v;
        }
    }
};

// ---------------------------------------------------------------- records
struct DK {  // dedup key: one per parsed event line
    u64 eh;
    i64 ingest;
    u64 loc;  // (thread << 48) | (person bucket << 32) | index, or NOLOC if the copy is invalid
};
static const u64 NOLOC = ~0ull;
struct PE {  // valid event copy, by person (32 bytes)
    u64 ph;
    const char* name;
    u32 ts;  // ms since window_start (the window is 14 days, far below 2^32 ms)
    u32 nlen;
    i32 price;
    u16 cat;
    u8 purch;
    u8 dead;
};
static_assert(sizeof(PE) == 32, "PE layout");
struct CK {  // valid click, by person
    u64 ph;
    i64 ts;
    const char* cid;
    u32 clen;
    u32 camp;
};
template <class T>
struct Span {
    T* d;
    size_t n;
    size_t size() const { return n; }
    T& operator[](size_t i) const { return d[i]; }
};
struct UR {  // user_daily row
    const char* name;
    u32 nlen;
    i32 day;
    i64 sessions, events, rev;
    i32 top;  // category index or -1
};

// ------------------------------------------------------------------ parquet
struct TW {
    string b;
    int last = 0;
    vector<int> st;
    void byte(int c) { b.push_back((char)c); }
    void varint(u64 v) {
        while (v >= 0x80) { byte((int)(v | 0x80) & 0xff); v >>= 7; }
        byte((int)v);
    }
    void zz(i64 v) { varint(((u64)v << 1) ^ (u64)(v >> 63)); }
    void fh(int id, int type) {
        int d = id - last;
        if (d > 0 && d <= 15) byte((d << 4) | type);
        else { byte(type); zz(id); }
        last = id;
    }
    void fi32(int id, i64 v) { fh(id, 5); zz(v); }
    void fi64(int id, i64 v) { fh(id, 6); zz(v); }
    void fstr(int id, const string& s) { fh(id, 8); varint(s.size()); b += s; }
    void sbeg(int id) { fh(id, 12); st.push_back(last); last = 0; }
    void ebeg() { st.push_back(last); last = 0; }  // struct as list element
    void send() { byte(0); last = st.back(); st.pop_back(); }
    void lhead(int id, int et, size_t n) {
        fh(id, 9);
        if (n < 15) byte((int)(n << 4) | et);
        else { byte(0xF0 | et); varint(n); }
    }
};
struct Col {
    string name;
    int type;  // 1 INT32, 2 INT64, 6 BYTE_ARRAY
    int conv;  // -1 none, 0 UTF8, 6 DATE
    bool optional = false;
    string vals;       // PLAIN-encoded non-null values
    vector<u8> defs;  // per row, when optional
};
static void put32(string& s, u32 v) { s.append((const char*)&v, 4); }
static void put64(string& s, u64 v) { s.append((const char*)&v, 8); }
static void write_parquet(const string& path, vector<Col>& cols, size_t nrows) {
    // Pieces are written in order straight from each column's buffer: no
    // concatenated copy of the file is ever built.
    FILE* fp = fopen(path.c_str(), "wb");
    if (!fp) { perror(path.c_str()); exit(3); }
    setvbuf(fp, nullptr, _IOFBF, 1 << 20);
    i64 pos = 0;
    auto put = [&](const void* p, size_t n) {
        if (n && fwrite(p, 1, n, fp) != n) { perror("write"); exit(3); }
        pos += (i64)n;
    };
    put("PAR1", 4);
    struct Meta { i64 off, size; };
    vector<Meta> meta;
    for (auto& c : cols) {
        string pre;  // definition levels (optional columns only)
        if (c.optional) {
            string rle;
            size_t i = 0;
            while (i < c.defs.size()) {
                size_t j = i;
                while (j < c.defs.size() && c.defs[j] == c.defs[i]) j++;
                u64 run = j - i;
                u64 h = run << 1;
                while (h >= 0x80) { rle.push_back((char)(h | 0x80)); h >>= 7; }
                rle.push_back((char)h);
                rle.push_back((char)c.defs[i]);
                i = j;
            }
            put32(pre, (u32)rle.size());
            pre += rle;
        }
        const i64 body = (i64)(pre.size() + c.vals.size());
        TW ph;
        ph.fi32(1, 0);  // DATA_PAGE
        ph.fi32(2, body);
        ph.fi32(3, body);
        ph.sbeg(5);
        ph.fi32(1, (i64)nrows);
        ph.fi32(2, 0);  // PLAIN
        ph.fi32(3, 3);  // RLE
        ph.fi32(4, 3);
        ph.send();
        ph.byte(0);
        i64 off = pos;
        put(ph.b.data(), ph.b.size());
        put(pre.data(), pre.size());
        put(c.vals.data(), c.vals.size());
        string().swap(c.vals);
        meta.push_back({off, (i64)ph.b.size() + body});
    }
    TW m;
    m.fi32(1, 1);
    m.lhead(2, 12, cols.size() + 1);
    m.ebeg();
    m.fstr(4, "schema");
    m.fi32(5, (i64)cols.size());
    m.send();
    for (auto& c : cols) {
        m.ebeg();
        m.fi32(1, c.type);
        m.fi32(3, c.optional ? 1 : 0);
        m.fstr(4, c.name);
        if (c.conv >= 0) m.fi32(6, c.conv);
        m.send();
    }
    m.fi64(3, (i64)nrows);
    m.lhead(4, 12, 1);
    m.ebeg();
    m.lhead(1, 12, cols.size());
    i64 total = 0;
    for (size_t k = 0; k < cols.size(); k++) {
        auto& c = cols[k];
        total += meta[k].size;
        m.ebeg();
        m.fi64(2, meta[k].off);
        m.sbeg(3);
        m.fi32(1, c.type);
        m.lhead(2, 5, 2);
        m.zz(0);
        m.zz(3);
        m.lhead(3, 8, 1);
        m.varint(c.name.size());
        m.b += c.name;
        m.fi32(4, 0);
        m.fi64(5, (i64)nrows);
        m.fi64(6, meta[k].size);
        m.fi64(7, meta[k].size);
        m.fi64(9, meta[k].off);
        m.send();
        m.send();
    }
    m.fi64(2, total);
    m.fi64(3, (i64)nrows);
    m.send();
    m.byte(0);
    string tailb = m.b;
    put32(tailb, (u32)m.b.size());
    tailb += "PAR1";
    put(tailb.data(), tailb.size());
    fclose(fp);
}
static void put_str(Col& c, const char* p, u32 n) {
    put32(c.vals, n);
    c.vals.append(p, n);
}

// ---------------------------------------------------------------------- main
static string env(const char* k, const char* d) {
    const char* v = getenv(k);
    return v && *v ? v : d;
}
static string json_str(const string& s, const string& key) {
    size_t k = s.find("\"" + key + "\"");
    if (k == string::npos) return "";
    size_t c = s.find(':', k);
    size_t a = s.find('"', c + 1);
    size_t b = s.find('"', a + 1);
    return s.substr(a + 1, b - a - 1);
}

int main(int argc, char** argv) {
#ifndef _WIN32
    // Back large allocations with transparent huge pages: far fewer page faults
    // and a faster exit. glibc reads this tunable only at startup, so re-exec once.
    if (!getenv("GLIBC_TUNABLES")) {
        setenv("GLIBC_TUNABLES", "glibc.malloc.hugetlb=1", 1);
        execv("/proc/self/exe", argv);  // returns only on failure: carry on without it
    }
#endif
#ifdef _WIN32
    if (const char* am = getenv("AFFINITY")) SetProcessAffinityMask(GetCurrentProcess(), strtoull(am, nullptr, 0));  // local benchmarking only
#endif
    PROF = getenv("PROFILE") != nullptr;
    EXP = atoi(env("EXP", "0").c_str());
    STOP = atoi(env("STOP", "0").c_str());
    string DATA = env("DATA_DIR", "/data"), OUT = env("OUTPUT_DIR", "/output");
    if (argc >= 3) { DATA = argv[1]; OUT = argv[2]; }
    NT = atoi(env("NCPU", "0").c_str());
    if (NT <= 0) NT = (int)thread::hardware_concurrency();
    if (NT <= 0) NT = 8;

    // manifest
    File mf = load(DATA + "/manifest.json");
    string ms(mf.data, mf.size);
    string ws = json_str(ms, "window_start"), we = json_str(ms, "window_end");
    const i64 WS = parse_ts(ws.data(), (u32)ws.size()), WE = parse_ts(we.data(), (u32)we.size());
    const i64 CWS = WS - WEEK;
    if (!getenv("NODATES") && ws.size() >= 4 && dg(ws[0]) && dg(ws[1]) && dg(ws[2]) && dg(ws[3])) init_dates(atoi(ws.substr(0, 4).c_str()) - 1);
    if (WE - WS > (i64)UINT32_MAX) { fputs("window too long\n", stderr); return 1; }
    const i64 DAY0 = fdiv(WS, DAY);
    const int NDAYS = (int)(fdiv(WE - 1, DAY) - DAY0 + 1);

    // products and categories
    File pf = load(DATA + "/products.csv");
    StrMap prod, catm;
    vector<View> catNames;
    {
        vector<pair<View, View>> rows;
        for_lines(pf, 0, pf.size, [&](const char* s, const char* le) {
            View f[2];
            if (split(s, le, 2, f)) rows.push_back({f[0], f[1]});
        });
        prod.init(rows.size());
        catm.init(64 + rows.size() / 100);
        for (auto& r : rows) {
            i32 c = catm.put(r.second.p, r.second.n, (i32)catNames.size());
            if (c == (i32)catNames.size()) catNames.push_back(r.second);
            prod.put(r.first.p, r.first.n, c);
        }
    }
    static const char UNK[] = "unknown";
    const u16 UNKC = (u16)catNames.size();
    catNames.push_back({UNK, 7});
    vector<int> catRank(catNames.size());
    {
        vector<int> o(catNames.size());
        for (size_t i = 0; i < o.size(); i++) o[i] = (int)i;
        sort(o.begin(), o.end(), [&](int a, int b) {
            const View &x = catNames[a], &y = catNames[b];
            int c = memcmp(x.p, y.p, min(x.n, y.n));
            return c ? c < 0 : x.n < y.n;
        });
        for (size_t r = 0; r < o.size(); r++) catRank[o[r]] = (int)r;
    }

    // campaigns: index 0 is organic
    File cf = load(DATA + "/campaigns.csv");
    StrMap campm;
    vector<View> campId{{"organic", 7}}, campCh{{"organic", 7}};
    {
        vector<pair<View, View>> rows;
        for_lines(cf, 0, cf.size, [&](const char* s, const char* le) {
            View f[2];
            if (split(s, le, 2, f)) rows.push_back({f[0], f[1]});
        });
        campm.init(rows.size());
        for (auto& r : rows) {
            i32 v = campm.put(r.first.p, r.first.n, (i32)campId.size());
            if (v == (i32)campId.size()) { campId.push_back(r.first); campCh.push_back(r.second); }
        }
    }
    const int NC = (int)campId.size();
    prof("dims");

    // load all part files
    vector<string> evn = list_csv(DATA + "/events"), ckn = list_csv(DATA + "/clicks"), lkn = list_csv(DATA + "/links");
    vector<File> evf(evn.size()), ckf(ckn.size()), lkf(lkn.size());
    {
        vector<pair<vector<File>*, pair<int, const string*>>> all;
        for (size_t i = 0; i < evn.size(); i++) all.push_back({&evf, {(int)i, &evn[i]}});
        for (size_t i = 0; i < ckn.size(); i++) all.push_back({&ckf, {(int)i, &ckn[i]}});
        for (size_t i = 0; i < lkn.size(); i++) all.push_back({&lkf, {(int)i, &lkn[i]}});
        parallel_for(all.size(), [&](size_t i, int) { (*all[i].first)[all[i].second.first] = load(*all[i].second.second); });
    }
    prof("open");

    // ------------------------------------------------------------ links
    // Links run on their own thread, one worker, while events and clicks parse:
    // nothing needs them until users are resolved after dedup.
    const int UBITS = 9, UB = 1 << UBITS;
    vector<u64> lmask(UB, 0);
    // per bucket, a cache-sized table: user hash -> final person hash and name
    struct RT { u64 h; u64 ph; const char* name; u32 n; u32 nlen; };
    vector<vector<RT>> rtab(UB);
    thread linker([&] {
        TL_NT = 1;
        // ------------------------------------------------------------ links
        // Every user ID is bucketed by the top bits of its hash (UB buckets). Each
        // bucket interns its own link IDs in a small table, so lookups stay in cache.
        struct LP { View a, b; };
        struct EP { u64 h; const char* p; u32 n; u32 side; u64 ref; };
        vector<vector<LP>> lparts(NT);
        vector<vector<EP>> eps((size_t)NT * UB);
        {
            vector<Chunk> ch;
            make_chunks(lkf, ch, 1 << 20);
            parallel_for(ch.size(), [&](size_t i, int t) {
                auto& o = lparts[t];
                for_lines(lkf[ch[i].file], ch[i].s, ch[i].e, [&](const char* s, const char* le) {
                    View f[2];
                    if (!split(s, le, 2, f) || !f[0].n || !f[1].n) return;
                    u64 ref = ((u64)t << 32) | o.size();
                    for (u32 k = 0; k < 2; k++) {
                        u64 h = hstr(f[k].p, f[k].n);
                        eps[(size_t)t * UB + (h >> (64 - UBITS))].push_back(EP{h, f[k].p, f[k].n, k, ref});
                    }
                    o.push_back({f[0], f[1]});
                });
            });
        }
        struct LT { u64 h; u32 n; u32 id; };
        vector<vector<LT>> ltab(UB);
        vector<vector<View>> lview(UB);
        vector<vector<array<u32, 2>>> ends(NT);
        for (int t = 0; t < NT; t++) ends[t].resize(lparts[t].size());
        parallel_for(UB, [&](size_t u, int) {
            size_t n = 0;
            for (int t = 0; t < NT; t++) n += eps[(size_t)t * UB + u].size();
            size_t cap = 16;
            while (cap < n * 2) cap <<= 1;
            auto& tb = ltab[u];
            tb.assign(cap, LT{0, 0, UINT32_MAX});
            const u64 m = cap - 1;
            lmask[u] = m;
            auto& lv = lview[u];
            for (int t = 0; t < NT; t++)
                for (const EP& e : eps[(size_t)t * UB + u]) {
                    u32 id;
                    for (u64 i = (e.h * 0x9E3779B97F4A7C15ull >> 20) & m;; i = (i + 1) & m) {
                        LT& x = tb[i];
                        if (x.id == UINT32_MAX) {
                            x = LT{e.h, e.n, (u32)lv.size()};
                            lv.push_back(View{e.p, e.n});
                            id = x.id;
                            break;
                        }
                        if (x.h == e.h && x.n == e.n) { id = x.id; break; }
                    }
                    ends[e.ref >> 32][e.ref & 0xffffffffu][e.side] = ((u32)u << 23) | id;
                }
        });
        vector<u32> lbase(UB + 1, 0);
        for (int u = 0; u < UB; u++) lbase[u + 1] = lbase[u] + (u32)lview[u].size();
        const u32 NID = lbase[UB];
        vector<View> lid(NID);
        for (int u = 0; u < UB; u++) copy(lview[u].begin(), lview[u].end(), lid.begin() + lbase[u]);
        vector<u32> par(NID);
        for (u32 i = 0; i < NID; i++) par[i] = i;
        auto findr = [&](u32 x) {
            while (par[x] != x) { par[x] = par[par[x]]; x = par[x]; }
            return x;
        };
        auto gid = [&](u32 e) { return lbase[e >> 23] + (e & 0x7fffff); };
        for (int t = 0; t < NT; t++)
            for (auto& e : ends[t]) {
                u32 a = findr(gid(e[0])), b = findr(gid(e[1]));
                if (a != b) { if (a < b) par[b] = a; else par[a] = b; }
            }
        for (u32 i = 0; i < NID; i++) par[i] = findr(i);
        // person of each linked id: smallest id in the component
        vector<u32> best(NID, UINT32_MAX);
        for (u32 i = 0; i < NID; i++) {
            u32& bb = best[par[i]];
            if (bb == UINT32_MAX) bb = i;
            else {
                const View &x = lid[i], &y = lid[bb];
                int c = memcmp(x.p, y.p, min(x.n, y.n));
                if (c ? c < 0 : x.n < y.n) bb = i;
            }
        }
        vector<u64> pnameh(NID);
        parallel_for(NT, [&](size_t t, int) {
            for (u32 i = (u32)t; i < NID; i += NT)
                if (best[i] != UINT32_MAX) pnameh[i] = hstr(lid[best[i]].p, lid[best[i]].n);
        });
        parallel_for(UB, [&](size_t u, int) {
            const auto& tb = ltab[u];
            auto& rt = rtab[u];
            rt.assign(tb.size(), RT{0, 0, nullptr, 0, UINT32_MAX});
            for (size_t i = 0; i < tb.size(); i++) {
                const LT& x = tb[i];
                if (x.id == UINT32_MAX) continue;
                u32 r = par[lbase[u] + x.id];
                const View& nm = lid[best[r]];
                rt[i] = RT{x.h, pnameh[r], nm.p, x.n, nm.n};
            }
            vector<LT>().swap(ltab[u]);
        });
        vector<vector<EP>>().swap(eps);
    });
    // resolve a user (hash uh, length n) to its person; unlinked users keep their own hash and name
    auto person = [&](u64 uh, u32 n, u64& ph, const char*& name, u32& nlen) {
        const u64 u = uh >> (64 - UBITS);
        const RT* tb = rtab[u].data();
        const u64 m = lmask[u];
        for (u64 i = (uh * 0x9E3779B97F4A7C15ull >> 20) & m;; i = (i + 1) & m) {
            const RT& x = tb[i];
            if (x.nlen == UINT32_MAX) return;
            if (x.h == uh && x.n == n) {
                ph = x.ph;
                name = x.name;
                nlen = x.nlen;
                return;
            }
        }
    };


    // ----------------------------------------------------- events + clicks
    const int EB = 256;  // event_id buckets
    const int PBITS = 9, PB = 1 << PBITS;  // person buckets
    vector<vector<DK>> kb((size_t)NT * EB);
    vector<vector<PE>> ub((size_t)NT * UB);
    vector<vector<CK>> cub((size_t)NT * UB);
    {
        size_t evbytes = 0;
        for (auto& f : evf) evbytes += f.size;
        size_t per = evbytes / 60 / ((size_t)NT * EB) * 5 / 4 + 16;
        size_t pp = evbytes / 64 / ((size_t)NT * PB) * 5 / 4 + 16;
        parallel_for(NT, [&](size_t t, int) {
            for (int b = 0; b < EB; b++) kb[t * EB + b].reserve(per);
            for (int b = 0; b < UB; b++) ub[t * UB + b].reserve(pp);
            if (EXP == 30) {  // experiment: fault in the reserved output pages before parsing
                for (int b = 0; b < EB; b++) { char* q = (char*)kb[t * EB + b].data(); size_t z = kb[t * EB + b].capacity() * sizeof(DK); for (size_t o = 0; o < z; o += 4096) q[o] = 0; }
                for (int b = 0; b < UB; b++) { char* q = (char*)ub[t * UB + b].data(); size_t z = ub[t * UB + b].capacity() * sizeof(PE); for (size_t o = 0; o < z; o += 4096) q[o] = 0; }
            }
        });
    }
    {
        vector<Chunk> ech, cch;
        make_chunks(evf, ech, 1 << 22);
        make_chunks(ckf, cch, 1 << 22);
        size_t ne = ech.size();
        auto parseTask = [&](size_t i, int t) {
            if (EXP == 16) return;
            if (EXP == 17) { const Chunk& c = i < ne ? ech[i] : cch[i - ne]; const File& F = i < ne ? evf[c.file] : ckf[c.file]; u64 acc = 0; for (size_t q = c.s; q < c.e; q += 64) acc += (u8)F.data[q]; nl_count += acc; return; }
            if (i < ne) {
                vector<DK>* KB = &kb[(size_t)t * EB];
                vector<PE>* B = &ub[(size_t)t * UB];
                for_fields<7>(evf[ech[i].file], ech[i].s, ech[i].e, [&](const View* f) {
                    if (f[0].n == 0) return;
                    if (STOP == 1) { nl_count += f[0].n; return; }
                    DK k;
                    k.eh = hstr(f[0].p, f[0].n);
                    if (STOP == 2) { nl_count += k.eh + hstr(f[1].p, f[1].n); return; }
                    i64 ing = parse_ts(f[3].p, f[3].n);
                    if (STOP == 3) { nl_count += k.eh + ing + parse_ts(f[2].p, f[2].n) + hstr(f[1].p, f[1].n); return; }
                    k.ingest = ing == BAD ? FAR : ing;
                    k.loc = NOLOC;
                    do {
                        if (f[1].n == 0 || f[5].n == 0) break;
                        i64 ts = parse_ts(f[2].p, f[2].n);
                        if (ts == BAD || ts < WS || ts >= WE) break;
                        PE e;
                        e.purch = 0;
                        e.price = 0;
                        e.cat = 0;
                        e.dead = 0;
                        const View& ty = f[4];
                        if (ty.n == 8 && memcmp(ty.p, "purchase", 8) == 0) {
                            const View& pr = f[6];
                            if (pr.n == 0) break;
                            i64 v = 0;
                            bool okp = true;
                            for (u32 q = 0; q < pr.n; q++) {
                                char c = pr.p[q];
                                if (!dg(c)) { okp = false; break; }
                                if (v <= 10000000) v = v * 10 + (c - '0');
                            }
                            if (!okp || v < 1 || v > 10000000) break;
                            e.purch = 1;
                            e.price = (i32)v;
                            i32 c = prod.get(f[5].p, f[5].n);
                            e.cat = c < 0 ? UNKC : (u16)c;
                        } else if (!(ty.n == 4 && (memcmp(ty.p, "view", 4) == 0 || memcmp(ty.p, "cart", 4) == 0)))
                            break;
                        if (k.ingest - ts > DAY) break;
                        e.ts = (u32)(ts - WS);
                        e.ph = hstr(f[1].p, f[1].n);  // user hash until resolved
                        e.name = f[1].p;
                        e.nlen = f[1].n;
                        u64 pp = e.ph >> (64 - UBITS);
                        auto& dst = B[pp];
                        k.loc = ((u64)t << 48) | (pp << 32) | (u64)dst.size();
                        if (STOP != 5) dst.push_back(e); else nl_count += e.ph;
                    } while (0);
                    if (STOP != 4 && STOP != 5) KB[k.eh >> 56].push_back(k); else nl_count += k.eh;
                });
            } else {
                const Chunk& c = cch[i - ne];
                vector<CK>* B = &cub[(size_t)t * UB];
                for_fields<4>(ckf[c.file], c.s, c.e, [&](const View* f) {
                    if (f[0].n == 0 || f[1].n == 0) return;
                    i64 ts = parse_ts(f[3].p, f[3].n);
                    if (ts == BAD || ts < CWS || ts >= WE) return;
                    i32 camp = campm.get(f[2].p, f[2].n);
                    if (camp < 0) return;
                    CK k;
                    k.ph = hstr(f[1].p, f[1].n);  // user hash until resolved
                    k.ts = ts;
                    k.cid = f[0].p;
                    k.clen = f[0].n;
                    k.camp = (u32)camp | (f[1].n << 16);  // campaign | user length, split in resolve
                    B[k.ph >> (64 - UBITS)].push_back(k);
                });
            }
        };
        // EXP=40: repeat the parse step in-process (buffers kept, so no page
        // faults after the first run) and print each run: a steadier measure.
        const int reps = EXP == 40 ? 7 : 1;
        for (int r = 0; r < reps; r++) {
            if (r) {
                for (auto& v : kb) v.clear();
                for (auto& v : ub) v.clear();
                for (auto& v : cub) v.clear();
            }
            auto a = chrono::steady_clock::now();
            parallel_for(ne + cch.size(), parseTask);
            if (EXP == 40) fprintf(stderr, "rep %d parse %.4f\n", r, chrono::duration<double>(chrono::steady_clock::now() - a).count());
        }
    }
    prof("parse");
    linker.join();
    prof("links");

    // ----------------------------------------------------- dedup events
    vector<vector<DK>> dtab(NT);
    vector<vector<u8>> dused(NT);
    parallel_for(EB, [&](size_t b, int t) {
        size_t n = 0;
        for (int s = 0; s < NT; s++) n += kb[(size_t)s * EB + b].size();
        size_t cap = 16;
        while (cap < n * 2) cap <<= 1;
        if (dtab[t].size() < cap) { dtab[t].resize(cap); dused[t].resize(cap); }  // reused across buckets
        DK* tab = dtab[t].data();
        u8* used = dused[t].data();
        memset(used, 0, cap);
        const u64 mask = cap - 1;
        auto kill = [&](u64 loc) {
            if (loc == NOLOC) return;
            ub[(size_t)(loc >> 48) * UB + ((loc >> 32) & 0xffff)][loc & 0xffffffffu].dead = 1;
        };
        for (int s = 0; s < NT; s++) {
            for (const DK& k : kb[(size_t)s * EB + b]) {
                for (u64 i = (k.eh * 0x9E3779B97F4A7C15ull >> 24) & mask;; i = (i + 1) & mask) {
                    if (!used[i]) { used[i] = 1; tab[i] = k; break; }
                    DK& e = tab[i];
                    if (e.eh == k.eh) {
                        if (k.ingest < e.ingest) { kill(e.loc); e = k; }
                        else kill(k.loc);
                        break;
                    }
                }
            }
        }
    });
    prof("dedup");

    // --------------------------------- resolve users to persons, regroup
    // Pass 1 resolves in place and counts per (user bucket, person bucket);
    // pass 2 scatters into one array per table, contiguous per person bucket.
    vector<u32> histE((size_t)UB * PB, 0), histC((size_t)UB * PB, 0);
    parallel_for(UB, [&](size_t u, int) {
        u32* hE = &histE[u * PB];
        u32* hC = &histC[u * PB];
        for (int s = 0; s < NT; s++) {
            for (PE& x : ub[(size_t)s * UB + u]) {
                if (x.dead) continue;
                person(x.ph, x.nlen, x.ph, x.name, x.nlen);
                hE[x.ph >> (64 - PBITS)]++;
            }
            for (CK& k : cub[(size_t)s * UB + u]) {
                const char* nm;
                u32 nn = k.camp >> 16;
                person(k.ph, nn, k.ph, nm, nn);
                k.camp &= 0xffff;
                hC[k.ph >> (64 - PBITS)]++;
            }
        }
    });
    prof("resolve1");
    vector<size_t> offE((size_t)UB * PB), offC((size_t)UB * PB), baseE(PB + 1), baseC(PB + 1);
    {
        size_t a = 0, c = 0;
        for (int p = 0; p < PB; p++) {
            baseE[p] = a;
            baseC[p] = c;
            for (int u = 0; u < UB; u++) {
                offE[(size_t)u * PB + p] = a;
                a += histE[(size_t)u * PB + p];
                offC[(size_t)u * PB + p] = c;
                c += histC[(size_t)u * PB + p];
            }
        }
        baseE[PB] = a;
        baseC[PB] = c;
    }
    PE* GE = new PE[baseE[PB] + 1];
    CK* GC = new CK[baseC[PB] + 1];
    if (EXP == 7) parallel_for(NT, [&](size_t t, int) {
        size_t n = baseE[PB] * sizeof(PE), a = n / NT * t, b = t + 1 == (size_t)NT ? n : n / NT * (t + 1);
        memset((char*)GE + a, 0, b - a);
    });
    prof("alloc");
    parallel_for(UB, [&](size_t u, int) {
        size_t* oE = &offE[u * PB];
        size_t* oC = &offC[u * PB];
        for (int s = 0; s < NT; s++) {
            auto& a = ub[(size_t)s * UB + u];
            for (const PE& x : a)
                if (!x.dead) GE[oE[x.ph >> (64 - PBITS)]++] = x;
            auto& c = cub[(size_t)s * UB + u];
            for (const CK& k : c) GC[oC[k.ph >> (64 - PBITS)]++] = k;
        }
    });
    prof("resolve");

    // ------------------------------------------- sessions + attribution
    const size_t CS = (size_t)NDAYS * NC;
    struct CStat { i64 sess, psess, rev, assist; };
    vector<vector<CStat>> cstat(NT, vector<CStat>(CS, CStat{0, 0, 0, 0}));
    vector<vector<UR>> urows(NT);
    vector<size_t> order(PB);
    {
        vector<size_t> sz(PB, 0);
        for (int p = 0; p < PB; p++) sz[p] = baseE[p + 1] - baseE[p];
        for (int p = 0; p < PB; p++) order[p] = p;
        sort(order.begin(), order.end(), [&](size_t a, size_t b) { return sz[a] > sz[b]; });
    }
    vector<vector<u32>> tcnt(NT, vector<u32>(NC, 0)), tpos(NT, vector<u32>(NC, 0));
    struct Scratch {
        vector<u64> hk;
        vector<u32> hv, pid, cpid, es, cs, w;
        vector<PE> eo;
        vector<CK> ko;
    };
    vector<Scratch> scr(NT);
    parallel_for(PB, [&](size_t oi, int t) {
        size_t p = order[oi];
        // Group by person (counting sort on a local person id), then sort each
        // person's events by ts and clicks by (ts, click_id). Persons come out in
        // first-seen order, the same order for events and clicks.
        Span<PE> E0{GE + baseE[p], baseE[p + 1] - baseE[p]};
        Span<CK> K0{GC + baseC[p], baseC[p + 1] - baseC[p]};
        Scratch& S = scr[t];
        const size_t n = E0.n, m = K0.n;
        size_t cap = 16;
        while (cap < n * 2) cap <<= 1;
        if (S.hk.size() < cap) { S.hk.resize(cap); S.hv.resize(cap); }
        u64* hk = S.hk.data();
        u32* hv = S.hv.data();
        memset(hv, 0xff, cap * sizeof(u32));
        const u64 hm = cap - 1;
        auto slot = [&](u64 ph) {
            for (u64 q = (ph * 0x9E3779B97F4A7C15ull >> 20) & hm;; q = (q + 1) & hm)
                if (hv[q] == UINT32_MAX || hk[q] == ph) return q;
        };
        if (S.pid.size() < n) S.pid.resize(n);
        u32 np = 0;
        for (size_t q = 0; q < n; q++) {
            u64 sl = slot(E0[q].ph);
            if (hv[sl] == UINT32_MAX) { hk[sl] = E0[q].ph; hv[sl] = np++; }
            S.pid[q] = hv[sl];
        }
        S.es.assign(np + 1, 0);
        for (size_t q = 0; q < n; q++) S.es[S.pid[q] + 1]++;
        for (u32 q = 0; q < np; q++) S.es[q + 1] += S.es[q];
        S.w.assign(S.es.begin(), S.es.end() - 1);
        if (S.eo.size() < n) S.eo.resize(n);
        for (size_t q = 0; q < n; q++) S.eo[S.w[S.pid[q]]++] = E0[q];
        if (S.cpid.size() < m) S.cpid.resize(m);
        S.cs.assign(np + 1, 0);
        for (size_t q = 0; q < m; q++) {
            u64 sl = slot(K0[q].ph);
            u32 id = hv[sl];
            S.cpid[q] = id;
            if (id != UINT32_MAX) S.cs[id + 1]++;
        }
        for (u32 q = 0; q < np; q++) S.cs[q + 1] += S.cs[q];
        S.w.assign(S.cs.begin(), S.cs.end() - 1);
        if (S.ko.size() < S.cs[np]) S.ko.resize(S.cs[np]);
        for (size_t q = 0; q < m; q++)
            if (S.cpid[q] != UINT32_MAX) S.ko[S.w[S.cpid[q]]++] = K0[q];
        if (EXP == 21) return;
        auto ckless = [](const CK& a, const CK& b) {
            if (a.ts != b.ts) return a.ts < b.ts;
            int c = memcmp(a.cid, b.cid, min(a.clen, b.clen));
            return c ? c < 0 : a.clen < b.clen;
        };
        for (u32 q = 0; q < np; q++) {
            PE* a = S.eo.data() + S.es[q];
            size_t len = S.es[q + 1] - S.es[q];
            if (len <= 24) {
                for (size_t x = 1; x < len; x++) {
                    PE v = a[x];
                    size_t y = x;
                    while (y > 0 && a[y - 1].ts > v.ts) { a[y] = a[y - 1]; y--; }
                    a[y] = v;
                }
            } else sort(a, a + len, [](const PE& x, const PE& y) { return x.ts < y.ts; });
            CK* c = S.ko.data() + S.cs[q];
            size_t cl = S.cs[q + 1] - S.cs[q];
            if (cl > 1) sort(c, c + cl, ckless);
        }
        if (EXP == 20) return;
        Span<PE> E{S.eo.data(), n};
        Span<CK> K{S.ko.data(), S.cs[np]};
        CStat* CSt = cstat[t].data();
        auto& U = urows[t];
        // Assists by intervals: a campaign is active for a run of consecutive
        // sessions while it has a click in the window. When it leaves (or the
        // person ends), its run is split by day and added in one step each,
        // instead of +1 per session per active campaign.
        vector<u32>& cnt = tcnt[t];
        vector<u32>& actS = tpos[t];  // session index at which the campaign became active
        u32 sidx = 0;                 // index of the person's current session
        vector<pair<i64, u32>> runs;  // (day, first session index), days ascending
        auto aflush = [&](u32 c, u32 a, u32 b) {
            for (size_t r = 0; r < runs.size() && a < b; r++) {
                u32 rlo = runs[r].second, rhi = r + 1 < runs.size() ? runs[r + 1].second : sidx;
                u32 x = max(a, rlo), y = min(b, rhi);
                if (y > x) CSt[(size_t)(runs[r].first - DAY0) * NC + c].assist += y - x;
            }
        };
        auto add = [&](u32 c) { if (cnt[c]++ == 0) actS[c] = sidx; };
        auto rem = [&](u32 c) { if (--cnt[c] == 0) aflush(c, actS[c], sidx); };
        vector<pair<u16, i64>> cats;
        size_t i = 0, ka = 0;
        while (i < E.size()) {
            const u64 ph = E[i].ph;
            size_t j = i + 1;
            while (j < E.size() && E[j].ph == ph) j++;
            size_t kb = ka;  // this person's clicks start at ka (same person order)
            while (kb < K.size() && K[kb].ph == ph) kb++;
            size_t lo = ka, hi = ka;
            UR row{E[i].name, E[i].nlen, 0, 0, 0, 0, -1};
            i64 curDay = INT64_MIN;
            cats.clear();
            sidx = 0;
            runs.clear();
            auto flush = [&]() {
                if (curDay == INT64_MIN) return;
                row.day = (i32)curDay;
                row.top = -1;
                i64 bestv = -1;
                for (auto& c : cats)
                    if (c.second > bestv || (c.second == bestv && catRank[c.first] < catRank[row.top])) {
                        bestv = c.second;
                        row.top = c.first;
                    }
                U.push_back(row);
                row.sessions = row.events = row.rev = 0;
                cats.clear();
            };
            size_t s = i;
            while (s < j) {
                const i64 start = WS + (i64)E[s].ts;
                const i64 day = fdiv(start, DAY);
                if (day != curDay) { flush(); curDay = day; runs.push_back({day, sidx}); }
                i64 prev = start, rev = 0;
                int np = 0;
                size_t e = s;
                while (e < j && WS + (i64)E[e].ts - prev <= GAP) {
                    const PE& x = E[e];
                    prev = WS + (i64)x.ts;
                    if (x.purch) {
                        rev += x.price;
                        np++;
                        bool f = false;
                        for (auto& c : cats)
                            if (c.first == x.cat) { c.second += x.price; f = true; break; }
                        if (!f) cats.push_back({x.cat, (i64)x.price});
                    }
                    e++;
                }
                row.sessions++;
                row.events += (i64)(e - s);
                row.rev += rev;
                // attribution
                while (hi < kb && K[hi].ts <= start) add(K[hi++].camp);
                while (lo < hi && K[lo].ts < start - WEEK) rem(K[lo++].camp);
                u32 camp = 0;
                if (hi > lo) {
                    size_t m = hi - 1;
                    const i64 tt = K[m].ts;
                    while (m > lo && K[m - 1].ts == tt) m--;
                    camp = K[m].camp;
                }
                const size_t di = (size_t)(day - DAY0) * NC;
                CStat& cs = CSt[di + camp];
                cs.sess++;
                cs.psess += np > 0;
                cs.rev += rev;
                sidx++;
                s = e;
            }
            flush();
            while (lo < hi) rem(K[lo++].camp);
            i = j;
            ka = kb;
        }
    });
    prof("sessions");

    // --------------------------------------------------------------- output
    fs::create_directories(OUT + "/user_daily");
    fs::create_directories(OUT + "/campaign_daily");
    vector<int> writers;
    for (int t = 0; t < NT; t++)
        if (t == 0 || !urows[t].empty()) writers.push_back(t);
    parallel_for(writers.size() + 1, [&](size_t w, int) {
        if (w == writers.size()) {
            vector<Col> cols(7);
            cols[0] = {"campaign_id", 6, 0};
            cols[1] = {"channel", 6, 0};
            cols[2] = {"day", 1, 6};
            cols[3] = {"sessions", 2, -1};
            cols[4] = {"purchasing_sessions", 2, -1};
            cols[5] = {"revenue_cents", 2, -1};
            cols[6] = {"assisted_sessions", 2, -1};
            size_t n = 0;
            for (int d = 0; d < NDAYS; d++)
                for (int c = 0; c < NC; c++) {
                    CStat sum{0, 0, 0, 0};
                    for (int t = 0; t < NT; t++) {
                        const CStat& x = cstat[t][(size_t)d * NC + c];
                        sum.sess += x.sess;
                        sum.psess += x.psess;
                        sum.rev += x.rev;
                        sum.assist += x.assist;
                    }
                    if (sum.sess == 0 && sum.assist == 0) continue;
                    if (c == 0) sum.assist = 0;
                    n++;
                    put_str(cols[0], campId[c].p, campId[c].n);
                    put_str(cols[1], campCh[c].p, campCh[c].n);
                    put32(cols[2].vals, (u32)(i32)(DAY0 + d));
                    put64(cols[3].vals, (u64)sum.sess);
                    put64(cols[4].vals, (u64)sum.psess);
                    put64(cols[5].vals, (u64)sum.rev);
                    put64(cols[6].vals, (u64)sum.assist);
                }
            write_parquet(OUT + "/campaign_daily/part-0.parquet", cols, n);
            return;
        }
        int t = writers[w];
        auto& U = urows[t];
        vector<Col> cols(6);
        cols[0] = {"user_id", 6, 0};
        cols[1] = {"day", 1, 6};
        cols[2] = {"sessions", 1, -1};  // INT32: any integer width is accepted
        cols[3] = {"events", 1, -1};
        cols[4] = {"revenue_cents", 2, -1};
        cols[5] = {"top_category", 6, 0, true};
        // exact sizes first, then fill each column through a raw pointer
        const size_t nr = U.size();
        size_t sb = 0, tb = 0, nt = 0;
        for (auto& r : U) {
            sb += r.nlen + 4;
            if (r.top >= 0) { tb += catNames[r.top].n + 4; nt++; }
        }
        cols[0].vals.resize(sb);
        cols[1].vals.resize(nr * 4);
        cols[2].vals.resize(nr * 4);
        cols[3].vals.resize(nr * 4);
        cols[4].vals.resize(nr * 8);
        cols[5].vals.resize(tb);
        cols[5].defs.resize(nr);
        char* p0 = &cols[0].vals[0];
        char* p5 = tb ? &cols[5].vals[0] : nullptr;
        i32* p1 = nr ? (i32*)&cols[1].vals[0] : nullptr;
        i32* p2 = nr ? (i32*)&cols[2].vals[0] : nullptr;
        i32* p3 = nr ? (i32*)&cols[3].vals[0] : nullptr;
        i64* p4 = nr ? (i64*)&cols[4].vals[0] : nullptr;
        u8* d5 = cols[5].defs.data();
        for (size_t q = 0; q < nr; q++) {
            const UR& r = U[q];
            memcpy(p0, &r.nlen, 4);
            memcpy(p0 + 4, r.name, r.nlen);
            p0 += 4 + r.nlen;
            p1[q] = r.day;
            p2[q] = (i32)r.sessions;
            p3[q] = (i32)r.events;
            p4[q] = r.rev;
            if (r.top >= 0) {
                const View& cn = catNames[r.top];
                memcpy(p5, &cn.n, 4);
                memcpy(p5 + 4, cn.p, cn.n);
                p5 += 4 + cn.n;
                d5[q] = 1;
            } else d5[q] = 0;
        }
        (void)nt;
        write_parquet(OUT + "/user_daily/part-" + to_string(t) + ".parquet", cols, U.size());
    });
    prof("write");
    fflush(stdout);
    std::_Exit(0);  // skip freeing gigabytes of memory
}
