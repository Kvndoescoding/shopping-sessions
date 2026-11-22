// Shopping Sessions pipeline (Rust, std only).
//
// Design, chosen to keep memory small and touch each event as few times as possible:
// - IDs of up to 10 chars from [0-9A-Za-z_] are packed into a u64 at 6 bits per
//   char, in an order-preserving code, so they compare like the strings. Other IDs
//   are interned (bit 63 set). No strings on the hot path.
// - Links are parsed first; persons are resolved while parsing events and clicks,
//   so each valid event is written once, straight into its person partition, as a
//   16-byte record. Dedup works on a side stream of 24-byte keys and marks losing
//   copies in place.
// - Input is read with positioned reads into per-thread buffers (no file data kept).
// - Sessions per partition: group by person (counting sort), sort each person by ts.
// - Hand-written Parquet; process exits without freeing anything.

use std::cell::UnsafeCell;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fs::{self, File};
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::sync::Mutex;
use std::time::Instant;

const DAY: i64 = 86_400_000;
const GAP: i64 = 1_800_000;
const WEEK: i64 = 7 * DAY;
const BAD: i64 = i64::MIN;
const FAR: i64 = i64::MAX;
const DROPPED: u64 = u64::MAX;
const PBITS: u32 = 10; // person partitions
const NP: usize = 1 << PBITS;
const KBITS: u32 = 8; // dedup key buckets
const NKB: usize = 1 << KBITS;
const LBITS: u32 = 8; // link-ID buckets
const NLB: usize = 1 << LBITS;
const CHUNK: u64 = 4 << 20;

// ---------------------------------------------------------------- utilities
#[inline(always)]
fn mix(x: u64) -> u64 {
    let mut z = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

struct Prof {
    on: bool,
    t0: Instant,
}
impl Prof {
    fn mark(&self, what: &str) {
        if self.on {
            eprintln!("{:7.3}  {}", self.t0.elapsed().as_secs_f64(), what);
        }
    }
}

/// Per-thread mutable state shared into a scoped thread pool; thread t only touches slot t.
struct PerThread<T>(Vec<UnsafeCell<T>>);
unsafe impl<T: Send> Sync for PerThread<T> {}
impl<T> PerThread<T> {
    fn new(v: Vec<T>) -> Self {
        PerThread(v.into_iter().map(UnsafeCell::new).collect())
    }
    #[allow(clippy::mut_from_ref)]
    unsafe fn get(&self, t: usize) -> &mut T {
        &mut *self.0[t].get()
    }
    fn into_inner(self) -> Vec<T> {
        self.0.into_iter().map(|c| c.into_inner()).collect()
    }
}

#[derive(Clone, Copy)]
struct SendPtr<T>(*mut T);
unsafe impl<T> Send for SendPtr<T> {}
unsafe impl<T> Sync for SendPtr<T> {}

fn par_for<F: Fn(usize, usize) + Sync>(nt: usize, n: usize, f: F) {
    let next = AtomicUsize::new(0);
    std::thread::scope(|s| {
        for t in 0..nt {
            let f = &f;
            let next = &next;
            s.spawn(move || loop {
                let i = next.fetch_add(1, Relaxed);
                if i >= n {
                    break;
                }
                f(i, t);
            });
        }
    });
}

// ------------------------------------------------------------------ ID keys
const fn build_code() -> [u8; 256] {
    let mut t = [0u8; 256];
    let mut v = 1u8;
    let mut b = 0usize;
    while b < 256 {
        let c = b as u8;
        if c.is_ascii_digit() || c.is_ascii_uppercase() || c == b'_' || c.is_ascii_lowercase() {
            t[b] = v;
            v += 1;
        }
        b += 1;
    }
    t
}
const fn build_dec() -> [u8; 64] {
    let code = build_code();
    let mut d = [0u8; 64];
    let mut b = 0usize;
    while b < 256 {
        if code[b] != 0 {
            d[code[b] as usize] = b as u8;
        }
        b += 1;
    }
    d
}
static CODE: [u8; 256] = build_code();
static DEC: [u8; 64] = build_dec();
const INTERNED: u64 = 1 << 63;

/// Order-preserving 6-bit packing; 0 if the ID cannot be packed (or is empty).
#[inline(always)]
fn pack(s: &[u8]) -> u64 {
    let n = s.len();
    if n == 0 || n > 10 {
        return 0;
    }
    let mut v = 0u64;
    let mut i = 0;
    while i < 10 {
        let c = if i < n {
            let c = CODE[s[i] as usize];
            if c == 0 {
                return 0;
            }
            c
        } else {
            0
        };
        v = (v << 6) | c as u64;
        i += 1;
    }
    v
}

struct Interner {
    m: Mutex<(HashMap<Vec<u8>, u64>, Vec<Vec<u8>>)>,
}
impl Interner {
    #[inline]
    fn key(&self, s: &[u8]) -> u64 {
        let p = pack(s);
        if p != 0 || s.is_empty() {
            return p;
        }
        let mut g = self.m.lock().unwrap();
        if let Some(&k) = g.0.get(s) {
            return k;
        }
        let k = INTERNED | g.1.len() as u64;
        g.1.push(s.to_vec());
        g.0.insert(s.to_vec(), k);
        k
    }
    fn names(&self) -> Vec<Vec<u8>> {
        self.m.lock().unwrap().1.clone()
    }
}
fn decode(k: u64, names: &[Vec<u8>], out: &mut Vec<u8>) {
    if k & INTERNED != 0 {
        out.extend_from_slice(&names[(k & !INTERNED) as usize]);
    } else {
        for i in (0..10).rev() {
            let c = ((k >> (6 * i)) & 63) as usize;
            if c == 0 {
                break;
            }
            out.push(DEC[c]);
        }
    }
}
fn key_cmp(a: u64, b: u64, names: &[Vec<u8>]) -> Ordering {
    if (a | b) & INTERNED == 0 {
        return a.cmp(&b);
    }
    let (mut x, mut y) = (Vec::new(), Vec::new());
    decode(a, names, &mut x);
    decode(b, names, &mut y);
    x.cmp(&y)
}

/// Open addressing u64 -> u64; key 0 marks an empty slot (keys are never 0).
struct U64Map {
    e: Vec<[u64; 2]>, // (key, value) side by side: one cache line holds both
    mask: usize,
}
impl U64Map {
    fn new(n: usize) -> Self {
        let mut c = 16;
        while c < n * 2 {
            c <<= 1;
        }
        U64Map { e: vec![[0, 0]; c], mask: c - 1 }
    }
    #[inline(always)]
    fn slot(&self, key: u64) -> usize {
        let mut i = (mix(key) as usize) & self.mask;
        loop {
            let kk = unsafe { self.e.get_unchecked(i)[0] };
            if kk == key || kk == 0 {
                return i;
            }
            i = (i + 1) & self.mask;
        }
    }
    #[inline(always)]
    fn get(&self, key: u64) -> Option<u64> {
        let i = self.slot(key);
        if self.e[i][0] == key {
            Some(self.e[i][1])
        } else {
            None
        }
    }
}

// --------------------------------------------------------------- file input
#[cfg(unix)]
fn read_at(f: &File, buf: &mut [u8], off: u64) -> usize {
    use std::os::unix::fs::FileExt;
    let mut got = 0;
    while got < buf.len() {
        match f.read_at(&mut buf[got..], off + got as u64) {
            Ok(0) => break,
            Ok(n) => got += n,
            Err(e) => panic!("read: {e}"),
        }
    }
    got
}
#[cfg(windows)]
fn read_at(f: &File, buf: &mut [u8], off: u64) -> usize {
    use std::os::windows::fs::FileExt;
    let mut got = 0;
    while got < buf.len() {
        match f.seek_read(&mut buf[got..], off + got as u64) {
            Ok(0) => break,
            Ok(n) => got += n,
            Err(e) => panic!("read: {e}"),
        }
    }
    got
}

struct InFile {
    f: File,
    size: u64,
}
struct Chunk {
    file: usize,
    s: u64,
    e: u64,
}
fn list_csv(dir: &str) -> Vec<InFile> {
    let mut paths: Vec<_> = match fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().map(|x| x == "csv").unwrap_or(false) && p.is_file())
            .collect(),
        Err(_) => Vec::new(),
    };
    paths.sort();
    paths
        .into_iter()
        .map(|p| {
            let f = File::open(&p).expect("open");
            let size = f.metadata().unwrap().len();
            InFile { f, size }
        })
        .collect()
}
fn make_chunks(files: &[InFile], out: &mut Vec<Chunk>, csz: u64) {
    for (i, f) in files.iter().enumerate() {
        let mut s = 0;
        while s < f.size {
            out.push(Chunk { file: i, s, e: (s + csz).min(f.size) });
            s += csz;
        }
    }
}
/// Reads the lines that start in [s, e) (skipping the file's header line) into buf.
/// Returns the range of buf holding exactly those complete lines.
fn chunk_lines(fl: &InFile, s: u64, e: u64, buf: &mut Vec<u8>) -> Option<(usize, usize)> {
    let rs = if s == 0 { 0 } else { s - 1 };
    let mut want = ((e + 8192).min(fl.size) - rs) as usize;
    loop {
        if buf.capacity() < want {
            buf.reserve(want - buf.len());
        }
        unsafe { buf.set_len(want) };
        let got = read_at(&fl.f, &mut buf[..want], rs);
        buf.truncate(got);
        let eof = rs + got as u64 >= fl.size;
        let b = &buf[..];
        let start = if s == 0 {
            match b.iter().position(|&c| c == b'\n') {
                Some(i) => i + 1,
                None => return None,
            }
        } else if b[0] == b'\n' {
            1
        } else {
            match b[1..].iter().position(|&c| c == b'\n') {
                Some(i) => i + 2,
                None => {
                    if eof {
                        return None;
                    }
                    want *= 2;
                    continue;
                }
            }
        };
        let lim = (e - rs) as usize;
        if start >= lim {
            return None;
        }
        // the last line starting before lim ends at the first newline at or after lim - 1
        match b[lim - 1..].iter().position(|&c| c == b'\n') {
            Some(i) => return Some((start, lim - 1 + i + 1)),
            None => {
                if eof {
                    return Some((start, b.len()));
                }
                want *= 2;
            }
        }
    }
}

// ---------------------------------------------------------------- CSV split
#[inline(always)]
fn bit_range(a: u32, b: u32) -> u64 {
    let hi = if b >= 64 { !0 } else { (1u64 << b) - 1 };
    let lo = if a >= 64 { !0 } else { (1u64 << a) - 1 };
    hi & !lo
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline(always)]
fn masks64(p: &[u8]) -> (u64, u64, u64) {
    use std::arch::x86_64::*;
    unsafe {
        let a = _mm256_loadu_si256(p.as_ptr() as *const __m256i);
        let b = _mm256_loadu_si256(p.as_ptr().add(32) as *const __m256i);
        let m = |v: i8| {
            let x = _mm256_set1_epi8(v);
            (_mm256_movemask_epi8(_mm256_cmpeq_epi8(a, x)) as u32 as u64)
                | ((_mm256_movemask_epi8(_mm256_cmpeq_epi8(b, x)) as u32 as u64) << 32)
        };
        (m(b',' as i8), m(b'"' as i8), m(b'\n' as i8))
    }
}
#[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
#[inline(always)]
fn masks64(p: &[u8]) -> (u64, u64, u64) {
    let (mut c, mut q, mut n) = (0u64, 0u64, 0u64);
    for i in 0..64 {
        let x = p[i];
        c |= ((x == b',') as u64) << i;
        q |= ((x == b'"') as u64) << i;
        n |= ((x == b'\n') as u64) << i;
    }
    (c, q, n)
}

/// Calls f with the (start, len) of exactly N fields for every well-formed line
/// of buf. A field is bare or wrapped in quotes as a whole; anything else drops
/// the line. One streaming pass over 64-byte blocks.
fn for_lines<const N: usize, F: FnMut(&[(usize, usize); N])>(buf: &[u8], mut f: F) {
    let n = buf.len();
    let mut fl = [(0usize, 0usize); N];
    let mut k = 0usize;
    let mut qc = 0u32;
    let mut bad = false;
    let mut fs = 0usize;
    let mut tail = [0u8; 64];
    let mut w = 0usize;
    macro_rules! emit {
        ($a:expr, $b:expr, $fq:expr) => {{
            let (a, b, fq) = ($a, $b, $fq);
            if k >= N {
                bad = true;
            } else {
                let len = b - a;
                if fq == 0 {
                    fl[k] = (a, len);
                } else if fq == 2 && len >= 2 && buf[a] == b'"' && buf[b - 1] == b'"' {
                    fl[k] = (a + 1, len - 2);
                } else {
                    bad = true;
                }
            }
            k += 1;
        }};
    }
    while w < n {
        let (c, q, nl) = if n - w >= 64 {
            masks64(&buf[w..w + 64])
        } else {
            let r = n - w;
            tail[..r].copy_from_slice(&buf[w..]);
            tail[r..].fill(0);
            let (c, q, nl) = masks64(&tail);
            let v = (1u64 << r) - 1;
            (c & v, q & v, nl & v)
        };
        let mut s = c | nl;
        let mut sb = if fs > w { (fs - w) as u32 } else { 0 };
        while s != 0 {
            let idx = s.trailing_zeros();
            s &= s - 1;
            emit!(fs, w + idx as usize, qc + (q & bit_range(sb, idx)).count_ones());
            fs = w + idx as usize + 1;
            sb = idx + 1;
            qc = 0;
            if (nl >> idx) & 1 == 1 {
                if !bad && k == N {
                    f(&fl);
                }
                k = 0;
                bad = false;
            }
        }
        qc += (q & bit_range(sb, 64)).count_ones();
        w += 64;
    }
    if fs < n {
        let fq = buf[fs..n].iter().filter(|&&b| b == b'"').count() as u32;
        emit!(fs, n, fq);
        if !bad && k == N {
            f(&fl);
        }
    }
}

// --------------------------------------------------------------- timestamps
fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u32;
    let mp = if m > 2 { m - 3 } else { m + 9 };
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe as i64 - 719468
}
fn dim(y: i32, m: u32) -> u32 {
    const D: [u32; 13] = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    if m == 2 && ((y % 4 == 0 && y % 100 != 0) || y % 400 == 0) {
        29
    } else {
        D[m as usize]
    }
}
struct Ts {
    y0: i32,
    dtab: Vec<i32>, // [year 0..4][month 0..12][day 0..32] -> days since epoch, or i32::MIN
}
impl Ts {
    fn new(y0: i32) -> Ts {
        let mut dtab = vec![i32::MIN; 4 * 12 * 32];
        for yi in 0..4 {
            for m in 1..=12u32 {
                for d in 1..=dim(y0 + yi, m) {
                    dtab[((yi as usize * 12 + m as usize - 1) * 32) + d as usize] =
                        days_from_civil((y0 + yi) as i64, m, d) as i32;
                }
            }
        }
        Ts { y0, dtab }
    }
    #[inline(always)]
    fn parse(&self, p: &[u8]) -> i64 {
        let n = p.len();
        if n == 0 {
            return BAD;
        }
        let dg = |c: u8| c.wrapping_sub(b'0') < 10;
        if dg(p[0]) && (n < 5 || dg(p[4])) {
            if p.iter().all(|&c| dg(c)) {
                let mut v: u64 = 0;
                for &c in p {
                    let d = (c - b'0') as u64;
                    if v > (i64::MAX as u64 - d) / 10 {
                        return FAR;
                    }
                    v = v * 10 + d;
                }
                return v as i64;
            }
        }
        if n != 20 && n != 25 {
            return BAD;
        }
        if !(dg(p[0]) && dg(p[1]) && dg(p[2]) && dg(p[3]) && p[4] == b'-' && dg(p[5]) && dg(p[6])
            && p[7] == b'-' && dg(p[8]) && dg(p[9]) && p[10] == b'T' && dg(p[11]) && dg(p[12])
            && p[13] == b':' && dg(p[14]) && dg(p[15]) && p[16] == b':' && dg(p[17]) && dg(p[18]))
        {
            return BAD;
        }
        let d2 = |i: usize| ((p[i] - b'0') * 10 + (p[i + 1] - b'0')) as i32;
        let mut off: i64 = 0;
        if n == 20 {
            if p[19] != b'Z' {
                return BAD;
            }
        } else {
            let sg = p[19];
            if (sg != b'+' && sg != b'-') || !dg(p[20]) || !dg(p[21]) || p[22] != b':' || !dg(p[23]) || !dg(p[24]) {
                return BAD;
            }
            let (oh, om) = (d2(20), d2(23));
            if oh > 23 || om > 59 {
                return BAD;
            }
            off = (oh as i64 * 60 + om as i64) * 60_000;
            if sg == b'-' {
                off = -off;
            }
        }
        let y = d2(0) * 100 + d2(2);
        let (mo, d, h, mi, s) = (d2(5), d2(8), d2(11), d2(14), d2(17));
        if h > 23 || mi > 59 || s > 59 {
            return BAD;
        }
        let yi = y.wrapping_sub(self.y0) as u32;
        let days: i64 = if yi < 4 && (1..=12).contains(&mo) {
            if d >= 32 {
                return BAD;
            }
            let dd = self.dtab[(yi as usize * 12 + mo as usize - 1) * 32 + d as usize];
            if dd == i32::MIN {
                return BAD;
            }
            dd as i64
        } else {
            if !(1..=12).contains(&mo) || d < 1 || d as u32 > dim(y, mo as u32) {
                return BAD;
            }
            days_from_civil(y as i64, mo as u32, d as u32)
        };
        ((days * 24 + h as i64) * 60 + mi as i64) * 60_000 + s as i64 * 1000 - off
    }
}

// ------------------------------------------------------------------ records
#[derive(Clone, Copy, Default)]
#[repr(C)]
struct Ev {
    person: u64, // DROPPED when a better copy of the event exists
    ts: u32,     // ms since window_start
    pc: u32,     // purchase: price | category << 24; otherwise 0
}
#[derive(Clone, Copy, Default)]
#[repr(C)]
struct Ck {
    person: u64,
    cid: u64,
    ts: u32,   // ms since window_start - 7 days
    camp: u32, // campaign index (>= 1)
}
const RING: usize = 16;
#[derive(Clone, Copy, Default)]
struct Pend {
    ukey: u64,
    h: u64,
    eid: u64,
    ingest: i64,
    ts: u32,
    pc: u32,
}
#[inline(always)]
fn prefetch(p: *const u8) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        std::arch::x86_64::_mm_prefetch(p as *const i8, std::arch::x86_64::_MM_HINT_T0);
    }
}
#[derive(Clone, Copy)]
#[repr(C)]
struct DKey {
    eid: u64,
    ingest: i64,
    loc: u64, // thread << 56 | partition << 40 | index, or u64::MAX for an invalid copy
}
#[derive(Clone, Copy)]
struct UR {
    person: u64,
    rev: i64,
    day: i32,
    sessions: i32,
    events: i32,
    top: i32,
}

// ------------------------------------------------------------------ parquet
struct TW {
    b: Vec<u8>,
    last: i32,
    st: Vec<i32>,
}
impl TW {
    fn new() -> TW {
        TW { b: Vec::new(), last: 0, st: Vec::new() }
    }
    fn byte(&mut self, c: u8) {
        self.b.push(c);
    }
    fn varint(&mut self, mut v: u64) {
        while v >= 0x80 {
            self.b.push((v as u8) | 0x80);
            v >>= 7;
        }
        self.b.push(v as u8);
    }
    fn zz(&mut self, v: i64) {
        self.varint(((v << 1) ^ (v >> 63)) as u64);
    }
    fn fh(&mut self, id: i32, ty: u8) {
        let d = id - self.last;
        if d > 0 && d <= 15 {
            self.byte(((d as u8) << 4) | ty);
        } else {
            self.byte(ty);
            self.zz(id as i64);
        }
        self.last = id;
    }
    fn i32f(&mut self, id: i32, v: i64) {
        self.fh(id, 5);
        self.zz(v);
    }
    fn i64f(&mut self, id: i32, v: i64) {
        self.fh(id, 6);
        self.zz(v);
    }
    fn strf(&mut self, id: i32, s: &[u8]) {
        self.fh(id, 8);
        self.varint(s.len() as u64);
        self.b.extend_from_slice(s);
    }
    fn sbeg(&mut self, id: i32) {
        self.fh(id, 12);
        self.st.push(self.last);
        self.last = 0;
    }
    fn ebeg(&mut self) {
        self.st.push(self.last);
        self.last = 0;
    }
    fn send(&mut self) {
        self.byte(0);
        self.last = self.st.pop().unwrap();
    }
    fn lhead(&mut self, id: i32, et: u8, n: usize) {
        self.fh(id, 9);
        if n < 15 {
            self.byte(((n as u8) << 4) | et);
        } else {
            self.byte(0xF0 | et);
            self.varint(n as u64);
        }
    }
}
struct Col {
    name: &'static str,
    ty: i64,   // 1 INT32, 2 INT64, 6 BYTE_ARRAY
    conv: i64, // -1 none, 0 UTF8, 6 DATE
    optional: bool,
    vals: Vec<u8>,
    defs: Vec<u8>,
}
impl Col {
    fn new(name: &'static str, ty: i64, conv: i64, optional: bool) -> Col {
        Col { name, ty, conv, optional, vals: Vec::new(), defs: Vec::new() }
    }
    fn put_str(&mut self, s: &[u8]) {
        self.vals.extend_from_slice(&(s.len() as u32).to_le_bytes());
        self.vals.extend_from_slice(s);
    }
}
fn write_parquet(path: &str, cols: &mut [Col], nrows: usize) {
    use std::io::Write;
    let mut out = std::io::BufWriter::with_capacity(1 << 20, File::create(path).expect("create output"));
    let mut pos: i64 = 0;
    let mut put = |out: &mut std::io::BufWriter<File>, b: &[u8], pos: &mut i64| {
        out.write_all(b).expect("write");
        *pos += b.len() as i64;
    };
    put(&mut out, b"PAR1", &mut pos);
    let mut meta = Vec::new();
    for c in cols.iter_mut() {
        let mut pre = Vec::new();
        if c.optional {
            let mut rle = Vec::new();
            let mut i = 0;
            while i < c.defs.len() {
                let mut j = i;
                while j < c.defs.len() && c.defs[j] == c.defs[i] {
                    j += 1;
                }
                let mut h = ((j - i) as u64) << 1;
                while h >= 0x80 {
                    rle.push((h as u8) | 0x80);
                    h >>= 7;
                }
                rle.push(h as u8);
                rle.push(c.defs[i]);
                i = j;
            }
            pre.extend_from_slice(&(rle.len() as u32).to_le_bytes());
            pre.extend_from_slice(&rle);
        }
        let body = (pre.len() + c.vals.len()) as i64;
        let mut ph = TW::new();
        ph.i32f(1, 0);
        ph.i32f(2, body);
        ph.i32f(3, body);
        ph.sbeg(5);
        ph.i32f(1, nrows as i64);
        ph.i32f(2, 0);
        ph.i32f(3, 3);
        ph.i32f(4, 3);
        ph.send();
        ph.byte(0);
        let off = pos;
        put(&mut out, &ph.b, &mut pos);
        put(&mut out, &pre, &mut pos);
        put(&mut out, &c.vals, &mut pos);
        meta.push((off, ph.b.len() as i64 + body));
    }
    let mut m = TW::new();
    m.i32f(1, 1);
    m.lhead(2, 12, cols.len() + 1);
    m.ebeg();
    m.strf(4, b"schema");
    m.i32f(5, cols.len() as i64);
    m.send();
    for c in cols.iter() {
        m.ebeg();
        m.i32f(1, c.ty);
        m.i32f(3, if c.optional { 1 } else { 0 });
        m.strf(4, c.name.as_bytes());
        if c.conv >= 0 {
            m.i32f(6, c.conv);
        }
        m.send();
    }
    m.i64f(3, nrows as i64);
    m.lhead(4, 12, 1);
    m.ebeg();
    m.lhead(1, 12, cols.len());
    let mut total = 0;
    for (k, c) in cols.iter().enumerate() {
        total += meta[k].1;
        m.ebeg();
        m.i64f(2, meta[k].0);
        m.sbeg(3);
        m.i32f(1, c.ty);
        m.lhead(2, 5, 2);
        m.zz(0);
        m.zz(3);
        m.lhead(3, 8, 1);
        m.varint(c.name.len() as u64);
        m.b.extend_from_slice(c.name.as_bytes());
        m.i32f(4, 0);
        m.i64f(5, nrows as i64);
        m.i64f(6, meta[k].1);
        m.i64f(7, meta[k].1);
        m.i64f(9, meta[k].0);
        m.send();
        m.send();
    }
    m.i64f(2, total);
    m.i64f(3, nrows as i64);
    m.send();
    m.byte(0);
    let mut tail = m.b.clone();
    tail.extend_from_slice(&(m.b.len() as u32).to_le_bytes());
    tail.extend_from_slice(b"PAR1");
    put(&mut out, &tail, &mut pos);
    out.flush().expect("flush");
}

// ------------------------------------------------------------- small tables
fn read_all(path: &str) -> Vec<u8> {
    fs::read(path).unwrap_or_default()
}
/// Rows of a small clean CSV (header skipped), as owned field vectors.
fn small_csv(path: &str) -> Vec<(Vec<u8>, Vec<u8>)> {
    let data = read_all(path);
    let start = match data.iter().position(|&c| c == b'\n') {
        Some(i) => i + 1,
        None => return Vec::new(),
    };
    let body = &data[start..];
    let mut rows = Vec::new();
    for_lines::<2, _>(body, |f| {
        rows.push((body[f[0].0..f[0].0 + f[0].1].to_vec(), body[f[1].0..f[1].0 + f[1].1].to_vec()));
    });
    rows
}

#[cfg(target_os = "linux")]
fn hugepage(v: &mut Vec<u8>) {
    extern "C" {
        fn madvise(addr: *mut u8, len: usize, advice: i32) -> i32;
    }
    let p = v.as_mut_ptr() as usize;
    let a = (p + 0x1F_FFFF) & !0x1F_FFFF;
    let end = (p + v.capacity()) & !0x1F_FFFF;
    if end > a {
        unsafe { madvise(a as *mut u8, end - a, 14) }; // MADV_HUGEPAGE
    }
}

#[cfg(unix)]
fn fast_exit() -> ! {
    extern "C" {
        fn _exit(code: i32) -> !;
    }
    unsafe { _exit(0) }
}
#[cfg(not(unix))]
fn fast_exit() -> ! {
    std::process::exit(0)
}

// --------------------------------------------------------------------- main
fn main() {
    let prof = Prof { on: std::env::var_os("PROFILE").is_some(), t0: Instant::now() };
    let args: Vec<String> = std::env::args().collect();
    let env = |k: &str, d: &str| std::env::var(k).ok().filter(|s| !s.is_empty()).unwrap_or_else(|| d.to_string());
    let (data, out) = if args.len() >= 3 {
        (args[1].clone(), args[2].clone())
    } else {
        (env("DATA_DIR", "/data"), env("OUTPUT_DIR", "/output"))
    };
    let mut nt: usize = env("NCPU", "0").parse().unwrap_or(0);
    if nt == 0 {
        nt = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(8);
    }

    // manifest
    let man = String::from_utf8_lossy(&read_all(&format!("{data}/manifest.json"))).to_string();
    let jstr = |key: &str| -> String {
        let k = man.find(&format!("\"{key}\"")).expect("manifest key");
        let c = k + man[k..].find(':').unwrap();
        let a = c + man[c..].find('"').unwrap() + 1;
        let b = a + man[a..].find('"').unwrap();
        man[a..b].to_string()
    };
    let (wss, wes) = (jstr("window_start"), jstr("window_end"));
    let ts0 = Ts::new(-100_000);
    let ws = ts0.parse(wss.as_bytes());
    let we = ts0.parse(wes.as_bytes());
    let y0 = wss.get(0..4).and_then(|s| s.parse::<i32>().ok()).unwrap_or(-100_000) - 1;
    let tsx = Ts::new(y0);
    let cws = ws - WEEK;
    assert!(we - ws <= u32::MAX as i64, "window too long");
    let day0 = ws.div_euclid(DAY);
    let ndays = ((we - 1).div_euclid(DAY) - day0 + 1) as usize;

    let interner = Interner { m: Mutex::new((HashMap::new(), Vec::new())) };

    // products and categories (index 255 is never used; "unknown" gets the next index)
    let mut cat_names: Vec<Vec<u8>> = Vec::new();
    let mut cat_idx: HashMap<Vec<u8>, u32> = HashMap::new();
    let prows = small_csv(&format!("{data}/products.csv"));
    let mut prod_packed = U64Map::new(prows.len());
    let mut prod_bytes: HashMap<Vec<u8>, u32> = HashMap::new();
    for (pid, cat) in &prows {
        let ci = *cat_idx.entry(cat.clone()).or_insert_with(|| {
            cat_names.push(cat.clone());
            (cat_names.len() - 1) as u32
        });
        let p = pack(pid);
        if p != 0 {
            let i = prod_packed.slot(p);
            if prod_packed.e[i][0] == 0 {
                prod_packed.e[i][0] = p;
                prod_packed.e[i][1] = ci as u64;
            }
        } else {
            prod_bytes.entry(pid.clone()).or_insert(ci);
        }
    }
    let unk = cat_names.len() as u32;
    cat_names.push(b"unknown".to_vec());
    assert!(cat_names.len() <= 255, "too many categories");
    let mut cat_rank = vec![0u32; cat_names.len()];
    {
        let mut o: Vec<usize> = (0..cat_names.len()).collect();
        o.sort_by(|&a, &b| cat_names[a].cmp(&cat_names[b]));
        for (r, &i) in o.iter().enumerate() {
            cat_rank[i] = r as u32;
        }
    }

    // campaigns: index 0 is organic
    let crows = small_csv(&format!("{data}/campaigns.csv"));
    let mut camp_id: Vec<Vec<u8>> = vec![b"organic".to_vec()];
    let mut camp_ch: Vec<Vec<u8>> = vec![b"organic".to_vec()];
    let mut camp_packed = U64Map::new(crows.len());
    let mut camp_bytes: HashMap<Vec<u8>, u32> = HashMap::new();
    for (cid, ch) in &crows {
        let p = pack(cid);
        let ix = camp_id.len() as u32;
        let fresh = if p != 0 {
            let i = camp_packed.slot(p);
            if camp_packed.e[i][0] == 0 {
                camp_packed.e[i][0] = p;
                camp_packed.e[i][1] = ix as u64;
                true
            } else {
                false
            }
        } else if !camp_bytes.contains_key(cid) {
            camp_bytes.insert(cid.clone(), ix);
            true
        } else {
            false
        };
        if fresh {
            camp_id.push(cid.clone());
            camp_ch.push(ch.clone());
        }
    }
    let nc = camp_id.len();
    prof.mark("dims");

    let evf = list_csv(&format!("{data}/events"));
    let ckf = list_csv(&format!("{data}/clicks"));
    let lkf = list_csv(&format!("{data}/links"));

    // ----------------------------------------------------------------- links
    struct LinkT {
        pairs: Vec<[u64; 2]>,
        eps: Vec<Vec<(u64, u32, u32)>>, // per bucket: (key, pair index, side)
        buf: Vec<u8>,
    }
    let lt = PerThread::new(
        (0..nt).map(|_| LinkT { pairs: Vec::new(), eps: (0..NLB).map(|_| Vec::new()).collect(), buf: Vec::new() }).collect(),
    );
    {
        let mut ch = Vec::new();
        make_chunks(&lkf, &mut ch, 1 << 20);
        par_for(nt, ch.len(), |i, t| {
            let st = unsafe { lt.get(t) };
            let c = &ch[i];
            let mut buf = std::mem::take(&mut st.buf);
            if let Some((a, b)) = chunk_lines(&lkf[c.file], c.s, c.e, &mut buf) {
                let body = &buf[a..b];
                for_lines::<2, _>(body, |f| {
                    if f[0].1 == 0 || f[1].1 == 0 {
                        return;
                    }
                    let ka = interner.key(&body[f[0].0..f[0].0 + f[0].1]);
                    let kb = interner.key(&body[f[1].0..f[1].0 + f[1].1]);
                    let pi = st.pairs.len() as u32;
                    st.pairs.push([ka, kb]);
                    st.eps[(mix(ka) >> (64 - LBITS)) as usize].push((ka, pi, 0));
                    st.eps[(mix(kb) >> (64 - LBITS)) as usize].push((kb, pi, 1));
                });
            }
            st.buf = buf;
        });
    }
    let mut lt = lt.into_inner();
    // per bucket: intern link IDs to local indices
    let mut ends: Vec<Vec<[u32; 2]>> = lt.iter().map(|x| vec![[0u32; 2]; x.pairs.len()]).collect();
    let ends_ptr: Vec<SendPtr<[u32; 2]>> = ends.iter_mut().map(|v| SendPtr(v.as_mut_ptr())).collect();
    let lmaps = PerThread::new((0..NLB).map(|_| U64Map::new(0)).collect::<Vec<_>>());
    let lkeys = PerThread::new((0..NLB).map(|_| Vec::<u64>::new()).collect::<Vec<_>>());
    par_for(nt, NLB, |b, _| {
        let n: usize = lt.iter().map(|x| x.eps[b].len()).sum();
        let m = unsafe { lmaps.get(b) };
        *m = U64Map::new(n);
        let keys = unsafe { lkeys.get(b) };
        for (t, x) in lt.iter().enumerate() {
            for &(k, pi, side) in &x.eps[b] {
                let i = m.slot(k);
                if m.e[i][0] == 0 {
                    m.e[i][0] = k;
                    m.e[i][1] = keys.len() as u64;
                    keys.push(k);
                }
                let local = m.e[i][1] as u32;
                unsafe { (*ends_ptr[t].0.add(pi as usize))[side as usize] = ((b as u32) << 24) | local };
            }
        }
    });
    for x in lt.iter_mut() {
        x.eps = Vec::new();
    }
    let mut lmaps = lmaps.into_inner();
    let lkeys = lkeys.into_inner();
    let mut lbase = vec![0u32; NLB + 1];
    for b in 0..NLB {
        lbase[b + 1] = lbase[b] + lkeys[b].len() as u32;
    }
    let nid = lbase[NLB] as usize;
    let mut lid: Vec<u64> = Vec::with_capacity(nid);
    for b in 0..NLB {
        lid.extend_from_slice(&lkeys[b]);
    }
    let link_names = interner.names();
    let mut par: Vec<u32> = (0..nid as u32).collect();
    fn find(par: &mut [u32], mut x: u32) -> u32 {
        while par[x as usize] != x {
            let g = par[par[x as usize] as usize];
            par[x as usize] = g;
            x = g;
        }
        x
    }
    let gid = |e: u32| lbase[(e >> 24) as usize] + (e & 0xFF_FFFF);
    for t in 0..nt {
        for e in &ends[t] {
            let a = find(&mut par, gid(e[0]));
            let b = find(&mut par, gid(e[1]));
            if a != b {
                // the root is always the byte-wise smallest ID: the person's name
                if key_cmp(lid[a as usize], lid[b as usize], &link_names) == Ordering::Less {
                    par[b as usize] = a;
                } else {
                    par[a as usize] = b;
                }
            }
        }
    }
    for i in 0..nid as u32 {
        let r = find(&mut par, i);
        par[i as usize] = r;
    }
    // the per-bucket maps now give each linked ID its person key directly
    {
        let lm = PerThread::new(std::mem::take(&mut lmaps));
        par_for(nt, NLB, |b, _| {
            let m = unsafe { lm.get(b) };
            let base = lbase[b];
            for i in 0..m.e.len() {
                if m.e[i][0] != 0 {
                    m.e[i][1] = lid[par[(base + m.e[i][1] as u32) as usize] as usize];
                }
            }
        });
        lmaps = lm.into_inner();
    }
    drop(lt);
    drop(ends);
    let has_links = nid > 0;
    let person_of = |k: u64| -> u64 {
        if !has_links {
            return k;
        }
        lmaps[(mix(k) >> (64 - LBITS)) as usize].get(k).unwrap_or(k)
    };
    prof.mark("links");

    // ------------------------------------------------------- events + clicks
    struct ParseT {
        ev: Vec<Vec<Ev>>,
        ck: Vec<Vec<Ck>>,
        keys: Vec<Vec<DKey>>,
        buf: Vec<u8>,
    }
    let evbytes: u64 = evf.iter().map(|f| f.size).sum();
    let ckbytes: u64 = ckf.iter().map(|f| f.size).sum();
    let per_ev = (evbytes / 70 / (nt * NP) as u64 * 5 / 4 + 16) as usize;
    let per_key = (evbytes / 70 / (nt * NKB) as u64 * 5 / 4 + 16) as usize;
    let per_ck = (ckbytes / 40 / (nt * NP) as u64 * 5 / 4 + 16) as usize;
    let pt = PerThread::new(
        (0..nt)
            .map(|_| ParseT { ev: Vec::new(), ck: Vec::new(), keys: Vec::new(), buf: Vec::with_capacity((CHUNK + 65536) as usize) })
            .collect::<Vec<_>>(),
    );
    par_for(nt, nt, |t, _| {
        let st = unsafe { pt.get(t) };
        st.ev = (0..NP).map(|_| Vec::with_capacity(per_ev)).collect();
        st.ck = (0..NP).map(|_| Vec::with_capacity(per_ck)).collect();
        st.keys = (0..NKB).map(|_| Vec::with_capacity(per_key)).collect();
    });
    {
        let mut ech = Vec::new();
        let mut cch = Vec::new();
        make_chunks(&evf, &mut ech, CHUNK);
        make_chunks(&ckf, &mut cch, CHUNK);
        let ne = ech.len();
        let prod_get = |s: &[u8]| -> u32 {
            let p = pack(s);
            let v = if p != 0 { prod_packed.get(p).map(|x| x as u32) } else { prod_bytes.get(s).copied() };
            v.unwrap_or(unk)
        };
        let camp_get = |s: &[u8]| -> Option<u32> {
            let p = pack(s);
            if p != 0 {
                camp_packed.get(p).map(|x| x as u32)
            } else {
                camp_bytes.get(s).copied()
            }
        };
        let stop: u32 = std::env::var("STOP").ok().and_then(|x| x.parse().ok()).unwrap_or(0);
        let reps: usize = std::env::var("REPS").ok().and_then(|x| x.parse().ok()).unwrap_or(1);
        for rep in 0..reps {
        if rep > 0 {
            for t in 0..nt {
                let st = unsafe { pt.get(t) };
                for v in st.ev.iter_mut() { v.clear(); }
                for v in st.ck.iter_mut() { v.clear(); }
                for v in st.keys.iter_mut() { v.clear(); }
            }
        }
        let t_rep = Instant::now();
        par_for(nt, ne + cch.len(), |i, t| {
            let st = unsafe { pt.get(t) };
            let mut buf = std::mem::take(&mut st.buf);
            if i < ne {
                let c = &ech[i];
                if let Some((a, b)) = chunk_lines(&evf[c.file], c.s, c.e, &mut buf) {
                    if stop == 1 { st.buf = buf; return; }
                    let body = &buf[a..b];
                    // Valid events wait in a small ring while their link-table slot is
                    // prefetched; the lookup finishes RING lines later, from cache.
                    let mut ring = [Pend::default(); RING];
                    let (mut rh, mut rn) = (0usize, 0usize);
                    macro_rules! complete {
                        ($q:expr) => {{
                            let q: Pend = $q;
                            let person = if has_links {
                                let m = &lmaps[(q.h >> (64 - LBITS)) as usize];
                                let mut i = (q.h as usize) & m.mask;
                                loop {
                                    let e = unsafe { *m.e.get_unchecked(i) };
                                    if e[0] == q.ukey {
                                        break e[1];
                                    }
                                    if e[0] == 0 {
                                        break q.ukey;
                                    }
                                    i = (i + 1) & m.mask;
                                }
                            } else {
                                q.ukey
                            };
                            let p = (mix(person) >> (64 - PBITS)) as usize;
                            let dst = &mut st.ev[p];
                            let loc = ((t as u64) << 56) | ((p as u64) << 40) | dst.len() as u64;
                            dst.push(Ev { person, ts: q.ts, pc: q.pc });
                            st.keys[(mix(q.eid) >> (64 - KBITS)) as usize].push(DKey { eid: q.eid, ingest: q.ingest, loc });
                        }};
                    }
                    for_lines::<7, _>(body, |f| {
                        if f[0].1 == 0 || stop == 2 {
                            return;
                        }
                        let fld = |j: usize| &body[f[j].0..f[j].0 + f[j].1];
                        let eid = interner.key(fld(0));
                        let ing = tsx.parse(fld(3));
                        let ingest = if ing == BAD { FAR } else { ing };
                        let loc = u64::MAX;
                        'valid: {
                            if f[1].1 == 0 || f[5].1 == 0 {
                                break 'valid;
                            }
                            let ts = tsx.parse(fld(2));
                            if ts == BAD || ts < ws || ts >= we {
                                break 'valid;
                            }
                            let ty = fld(4);
                            let mut pc = 0u32;
                            if ty == b"purchase" {
                                let pr = fld(6);
                                if pr.is_empty() {
                                    break 'valid;
                                }
                                let mut v: u64 = 0;
                                for &ch in pr {
                                    if !ch.is_ascii_digit() {
                                        break 'valid;
                                    }
                                    if v <= 10_000_000 {
                                        v = v * 10 + (ch - b'0') as u64;
                                    }
                                }
                                if !(1..=10_000_000).contains(&v) {
                                    break 'valid;
                                }
                                pc = v as u32 | (prod_get(fld(5)) << 24);
                            } else if ty != b"view" && ty != b"cart" {
                                break 'valid;
                            }
                            if ingest - ts > DAY {
                                break 'valid;
                            }
                            let ukey = interner.key(fld(1));
                            let h = mix(ukey);
                            if has_links {
                                let m = &lmaps[(h >> (64 - LBITS)) as usize];
                                prefetch(unsafe { m.e.as_ptr().add((h as usize) & m.mask) } as *const u8);
                            }
                            let pend = Pend { ukey, h, eid, ingest, ts: (ts - ws) as u32, pc };
                            if rn == RING {
                                let old = ring[rh];
                                ring[rh] = pend;
                                rh = (rh + 1) % RING;
                                complete!(old);
                            } else {
                                ring[(rh + rn) % RING] = pend;
                                rn += 1;
                            }
                            return;
                        }
                        st.keys[(mix(eid) >> (64 - KBITS)) as usize].push(DKey { eid, ingest, loc });
                    });
                    for k in 0..rn {
                        complete!(ring[(rh + k) % RING]);
                    }
                }
            } else {
                let c = &cch[i - ne];
                if let Some((a, b)) = chunk_lines(&ckf[c.file], c.s, c.e, &mut buf) {
                    let body = &buf[a..b];
                    for_lines::<4, _>(body, |f| {
                        if f[0].1 == 0 || f[1].1 == 0 {
                            return;
                        }
                        let fld = |j: usize| &body[f[j].0..f[j].0 + f[j].1];
                        let ts = tsx.parse(fld(3));
                        if ts == BAD || ts < cws || ts >= we {
                            return;
                        }
                        let camp = match camp_get(fld(2)) {
                            Some(c) => c,
                            None => return,
                        };
                        let person = person_of(interner.key(fld(1)));
                        let cid = interner.key(fld(0));
                        let p = (mix(person) >> (64 - PBITS)) as usize;
                        st.ck[p].push(Ck { person, cid, ts: (ts - cws) as u32, camp });
                    });
                }
            }
            st.buf = buf;
        });
        if reps > 1 { eprintln!("rep {} parse {:.4}", rep, t_rep.elapsed().as_secs_f64()); }
        }
    }
    let mut pt = pt.into_inner();
    prof.mark("parse");

    // ------------------------------------------------------------------ dedup
    let evptr: Vec<Vec<SendPtr<Ev>>> =
        pt.iter_mut().map(|x| x.ev.iter_mut().map(|v| SendPtr(v.as_mut_ptr())).collect()).collect();
    {
        struct DT {
            eid: Vec<u64>,
            ing: Vec<i64>,
            loc: Vec<u64>,
        }
        let dt = PerThread::new((0..nt).map(|_| DT { eid: Vec::new(), ing: Vec::new(), loc: Vec::new() }).collect());
        let ptr = &pt;
        par_for(nt, NKB, |b, t| {
            let d = unsafe { dt.get(t) };
            let n: usize = ptr.iter().map(|x| x.keys[b].len()).sum();
            let mut cap = 16;
            while cap < n * 2 {
                cap <<= 1;
            }
            if d.eid.len() < cap {
                d.eid.resize(cap, 0);
                d.ing.resize(cap, 0);
                d.loc.resize(cap, 0);
            }
            d.eid[..cap].fill(0);
            let mask = cap - 1;
            let kill = |loc: u64| {
                if loc != u64::MAX {
                    let (tt, pp, ix) = ((loc >> 56) as usize, ((loc >> 40) & 0xFFFF) as usize, (loc & 0xFF_FFFF_FFFF) as usize);
                    unsafe { (*evptr[tt][pp].0.add(ix)).person = DROPPED };
                }
            };
            for x in ptr.iter() {
                for k in &x.keys[b] {
                    let mut i = (mix(k.eid) as usize) & mask;
                    loop {
                        let e = d.eid[i];
                        if e == 0 {
                            d.eid[i] = k.eid;
                            d.ing[i] = k.ingest;
                            d.loc[i] = k.loc;
                            break;
                        }
                        if e == k.eid {
                            if k.ingest < d.ing[i] {
                                kill(d.loc[i]);
                                d.ing[i] = k.ingest;
                                d.loc[i] = k.loc;
                            } else {
                                kill(k.loc);
                            }
                            break;
                        }
                        i = (i + 1) & mask;
                    }
                }
            }
        });
    }
    for x in pt.iter_mut() {
        x.keys = Vec::new();
    }
    prof.mark("dedup");

    // --------------------------------------------- sessions + attribution
    let names = interner.names();
    let cs_len = ndays * nc;
    let mut sizes: Vec<(usize, usize)> = (0..NP).map(|p| (pt.iter().map(|x| x.ev[p].len()).sum::<usize>(), p)).collect();
    sizes.sort_unstable_by(|a, b| b.cmp(a));
    struct ST {
        e: Vec<Ev>,
        k: Vec<Ck>,
        hk: Vec<u64>,
        hv: Vec<u32>,
        pid: Vec<u32>,
        es: Vec<u32>,
        cs: Vec<u32>,
        w: Vec<u32>,
        eo: Vec<Ev>,
        ko: Vec<Ck>,
        cnt: Vec<u32>,
        acts: Vec<u32>,
        cstat: Vec<[i64; 4]>, // sessions, purchasing, revenue, assisted
        rows: Vec<UR>,
    }
    let stt = PerThread::new(
        (0..nt)
            .map(|_| ST {
                e: Vec::new(),
                k: Vec::new(),
                hk: Vec::new(),
                hv: Vec::new(),
                pid: Vec::new(),
                es: Vec::new(),
                cs: Vec::new(),
                w: Vec::new(),
                eo: Vec::new(),
                ko: Vec::new(),
                cnt: vec![0; nc],
                acts: vec![0; nc],
                cstat: vec![[0; 4]; cs_len],
                rows: Vec::new(),
            })
            .collect(),
    );
    {
        let ptr = &pt;
        let names = &names;
        let cat_rank = &cat_rank;
        par_for(nt, NP, |oi, t| {
            let p = sizes[oi].1;
            let s = unsafe { stt.get(t) };
            s.e.clear();
            for x in ptr.iter() {
                for ev in &x.ev[p] {
                    if ev.person != DROPPED {
                        s.e.push(*ev);
                    }
                }
            }
            s.k.clear();
            for x in ptr.iter() {
                s.k.extend_from_slice(&x.ck[p]);
            }
            let n = s.e.len();
            if n == 0 {
                return;
            }
            // local person ids (first-seen order), counting sort
            let mut cap = 16;
            while cap < n * 2 {
                cap <<= 1;
            }
            if s.hk.len() < cap {
                s.hk.resize(cap, 0);
                s.hv.resize(cap, 0);
            }
            s.hv[..cap].fill(u32::MAX);
            let hm = cap - 1;
            let slot = |hk: &[u64], hv: &[u32], ph: u64| {
                let mut q = (mix(ph) as usize) & hm;
                loop {
                    if hv[q] == u32::MAX || hk[q] == ph {
                        return q;
                    }
                    q = (q + 1) & hm;
                }
            };
            s.pid.clear();
            let mut np = 0u32;
            for q in 0..n {
                let ph = s.e[q].person;
                let sl = slot(&s.hk, &s.hv, ph);
                if s.hv[sl] == u32::MAX {
                    s.hk[sl] = ph;
                    s.hv[sl] = np;
                    np += 1;
                }
                s.pid.push(s.hv[sl]);
            }
            s.es.clear();
            s.es.resize(np as usize + 1, 0);
            for &id in &s.pid {
                s.es[id as usize + 1] += 1;
            }
            for q in 0..np as usize {
                s.es[q + 1] += s.es[q];
            }
            s.w.clear();
            s.w.extend_from_slice(&s.es[..np as usize]);
            s.eo.clear();
            s.eo.resize(n, Ev::default());
            for q in 0..n {
                let id = s.pid[q] as usize;
                let at = s.w[id] as usize;
                s.eo[at] = s.e[q];
                s.w[id] += 1;
            }
            // clicks of these persons, same order
            s.cs.clear();
            s.cs.resize(np as usize + 1, 0);
            s.pid.clear();
            for q in 0..s.k.len() {
                let sl = slot(&s.hk, &s.hv, s.k[q].person);
                let id = s.hv[sl];
                s.pid.push(id);
                if id != u32::MAX {
                    s.cs[id as usize + 1] += 1;
                }
            }
            for q in 0..np as usize {
                s.cs[q + 1] += s.cs[q];
            }
            s.w.clear();
            s.w.extend_from_slice(&s.cs[..np as usize]);
            s.ko.clear();
            s.ko.resize(s.cs[np as usize] as usize, Ck::default());
            for q in 0..s.k.len() {
                let id = s.pid[q];
                if id != u32::MAX {
                    let at = s.w[id as usize] as usize;
                    s.ko[at] = s.k[q];
                    s.w[id as usize] += 1;
                }
            }
            let ck_less = |a: &Ck, b: &Ck| -> Ordering {
                if a.ts != b.ts {
                    a.ts.cmp(&b.ts)
                } else {
                    key_cmp(a.cid, b.cid, names)
                }
            };
            let day0_ = day0;
            for q in 0..np as usize {
                let ev = &mut s.eo[s.es[q] as usize..s.es[q + 1] as usize];
                if ev.len() <= 24 {
                    for x in 1..ev.len() {
                        let v = ev[x];
                        let mut y = x;
                        while y > 0 && ev[y - 1].ts > v.ts {
                            ev[y] = ev[y - 1];
                            y -= 1;
                        }
                        ev[y] = v;
                    }
                } else {
                    ev.sort_unstable_by_key(|e| e.ts);
                }
                let ck = &mut s.ko[s.cs[q] as usize..s.cs[q + 1] as usize];
                if ck.len() > 1 {
                    ck.sort_unstable_by(ck_less);
                }
            }
            // per person: sessions, user rows, credit, assists
            let ST { eo, ko, es, cs, cnt, acts, cstat, rows, .. } = s;
            let mut cats: Vec<(u32, i64)> = Vec::new();
            let mut runs: Vec<(i64, u32)> = Vec::new();
            for q in 0..np as usize {
                let ev = &eo[es[q] as usize..es[q + 1] as usize];
                let ck = &ko[cs[q] as usize..cs[q + 1] as usize];
                let person = ev[0].person;
                let mut row = UR { person, rev: 0, day: 0, sessions: 0, events: 0, top: -1 };
                let mut cur_day = i64::MIN;
                cats.clear();
                runs.clear();
                let mut sidx: u32 = 0;
                let (mut lo, mut hi) = (0usize, 0usize);
                macro_rules! flush {
                    () => {
                        if cur_day != i64::MIN {
                            row.day = cur_day as i32;
                            row.top = -1;
                            let mut best = -1i64;
                            for &(c, v) in &cats {
                                if v > best || (v == best && cat_rank[c as usize] < cat_rank[row.top as usize]) {
                                    best = v;
                                    row.top = c as i32;
                                }
                            }
                            rows.push(row);
                            row.sessions = 0;
                            row.events = 0;
                            row.rev = 0;
                            cats.clear();
                        }
                    };
                }
                let aflush = |cstat: &mut Vec<[i64; 4]>, runs: &Vec<(i64, u32)>, c: u32, a: u32, b: u32, sidx: u32| {
                    let mut r = 0;
                    while r < runs.len() && a < b {
                        let rlo = runs[r].1;
                        let rhi = if r + 1 < runs.len() { runs[r + 1].1 } else { sidx };
                        let x = a.max(rlo);
                        let y = b.min(rhi);
                        if y > x {
                            cstat[(runs[r].0 - day0_) as usize * nc + c as usize][3] += (y - x) as i64;
                        }
                        r += 1;
                    }
                };
                let mut sx = 0usize;
                while sx < ev.len() {
                    let start = ws + ev[sx].ts as i64;
                    let day = start.div_euclid(DAY);
                    if day != cur_day {
                        flush!();
                        cur_day = day;
                        runs.push((day, sidx));
                    }
                    let mut prev = start;
                    let mut rev = 0i64;
                    let mut np_ = 0;
                    let mut e = sx;
                    while e < ev.len() && ws + ev[e].ts as i64 - prev <= GAP {
                        let x = ev[e];
                        prev = ws + x.ts as i64;
                        if x.pc != 0 {
                            let price = (x.pc & 0xFF_FFFF) as i64;
                            let cat = x.pc >> 24;
                            rev += price;
                            np_ += 1;
                            match cats.iter_mut().find(|c| c.0 == cat) {
                                Some(c) => c.1 += price,
                                None => cats.push((cat, price)),
                            }
                        }
                        e += 1;
                    }
                    row.sessions += 1;
                    row.events += (e - sx) as i32;
                    row.rev += rev;
                    // the click window [start - 7 days, start]
                    while hi < ck.len() && cws + ck[hi].ts as i64 <= start {
                        let c = ck[hi].camp as usize;
                        if cnt[c] == 0 {
                            acts[c] = sidx;
                        }
                        cnt[c] += 1;
                        hi += 1;
                    }
                    while lo < hi && cws + (ck[lo].ts as i64) < start - WEEK {
                        let c = ck[lo].camp as usize;
                        cnt[c] -= 1;
                        if cnt[c] == 0 {
                            aflush(cstat, &runs, c as u32, acts[c], sidx, sidx);
                        }
                        lo += 1;
                    }
                    let mut camp = 0usize;
                    if hi > lo {
                        let mut m = hi - 1;
                        let tt = ck[m].ts;
                        while m > lo && ck[m - 1].ts == tt {
                            m -= 1;
                        }
                        camp = ck[m].camp as usize;
                    }
                    let cs_ = &mut cstat[(day - day0_) as usize * nc + camp];
                    cs_[0] += 1;
                    cs_[1] += (np_ > 0) as i64;
                    cs_[2] += rev;
                    sidx += 1;
                    sx = e;
                }
                flush!();
                while lo < hi {
                    let c = ck[lo].camp as usize;
                    cnt[c] -= 1;
                    if cnt[c] == 0 {
                        aflush(cstat, &runs, c as u32, acts[c], sidx, sidx);
                    }
                    lo += 1;
                }
            }
        });
    }
    let stt = stt.into_inner();
    prof.mark("sessions");

    // ----------------------------------------------------------------- output
    let _ = fs::create_dir_all(format!("{out}/user_daily"));
    let _ = fs::create_dir_all(format!("{out}/campaign_daily"));
    let writers: Vec<usize> = (0..nt).filter(|&t| t == 0 || !stt[t].rows.is_empty()).collect();
    par_for(nt.max(2), writers.len() + 1, |w, _| {
        if w == writers.len() {
            let mut cols = vec![
                Col::new("campaign_id", 6, 0, false),
                Col::new("channel", 6, 0, false),
                Col::new("day", 1, 6, false),
                Col::new("sessions", 2, -1, false),
                Col::new("purchasing_sessions", 2, -1, false),
                Col::new("revenue_cents", 2, -1, false),
                Col::new("assisted_sessions", 2, -1, false),
            ];
            let mut n = 0;
            for d in 0..ndays {
                for c in 0..nc {
                    let mut sum = [0i64; 4];
                    for s in &stt {
                        let x = s.cstat[d * nc + c];
                        for j in 0..4 {
                            sum[j] += x[j];
                        }
                    }
                    if sum[0] == 0 && sum[3] == 0 {
                        continue;
                    }
                    if c == 0 {
                        sum[3] = 0;
                    }
                    n += 1;
                    cols[0].put_str(&camp_id[c]);
                    cols[1].put_str(&camp_ch[c]);
                    cols[2].vals.extend_from_slice(&((day0 + d as i64) as i32).to_le_bytes());
                    for j in 0..4 {
                        cols[3 + j].vals.extend_from_slice(&sum[j].to_le_bytes());
                    }
                }
            }
            write_parquet(&format!("{out}/campaign_daily/part-0.parquet"), &mut cols, n);
            return;
        }
        let t = writers[w];
        let rows = &stt[t].rows;
        let mut cols = vec![
            Col::new("user_id", 6, 0, false),
            Col::new("day", 1, 6, false),
            Col::new("sessions", 1, -1, false),
            Col::new("events", 1, -1, false),
            Col::new("revenue_cents", 2, -1, false),
            Col::new("top_category", 6, 0, true),
        ];
        let nr = rows.len();
        cols[0].vals.reserve(nr * 14);
        cols[1].vals.reserve(nr * 4);
        cols[2].vals.reserve(nr * 4);
        cols[3].vals.reserve(nr * 4);
        cols[4].vals.reserve(nr * 8);
        cols[5].defs.reserve(nr);
        let mut nb = Vec::with_capacity(64);
        for r in rows {
            nb.clear();
            decode(r.person, &names, &mut nb);
            cols[0].put_str(&nb);
            cols[1].vals.extend_from_slice(&r.day.to_le_bytes());
            cols[2].vals.extend_from_slice(&r.sessions.to_le_bytes());
            cols[3].vals.extend_from_slice(&r.events.to_le_bytes());
            cols[4].vals.extend_from_slice(&r.rev.to_le_bytes());
            if r.top >= 0 {
                cols[5].defs.push(1);
                cols[5].put_str(&cat_names[r.top as usize]);
            } else {
                cols[5].defs.push(0);
            }
        }
        write_parquet(&format!("{out}/user_daily/part-{t}.parquet"), &mut cols, nr);
    });
    prof.mark("write");
    fast_exit();
}
