# Shopping Sessions: native Rust entry

A Builders Buildathon entry for the Shopping Sessions track. It turns the click-stream CSVs in `/data` into the two daily Parquet tables, `/output/user_daily/` and `/output/campaign_daily/`, following the track's `rules.md`.

The pipeline is one Rust file, `src/main.rs`, using only the standard library. The `Dockerfile` compiles it into one binary. The earlier C++ entry, which gives the same output, is kept in `cpp/`.

## How it works

1. **IDs as integers.** IDs of up to 10 characters from `[0-9A-Za-z_]` are packed into a `u64` at 6 bits per character, in an order-preserving code, so they compare like the strings. Longer or unusual IDs are interned instead. No strings are handled on the hot path.
2. **Links first.** Linked IDs are bucketed by hash and joined with union-find. Each person is named by the byte-wise smallest of their IDs. The result is a set of small open-addressing tables, one per bucket, that map a user ID to its person.
3. **Parse.** Events and clicks are split into 4 MB chunks that start at line boundaries and are read with positioned reads into per-thread buffers. AVX2 finds field and line breaks. Timestamps and prices are checked by hand. Each valid event looks up its person while it is being parsed: the table slot is prefetched and the lookup finishes 16 lines later, once the data is in cache. The event is then written once, as a 16-byte record, straight into its person's partition.
4. **Dedup.** Each event line also emits a 24-byte key: its `event_id`, its `ingest_ts`, and where its record lives. Keys are bucketed by hash. The copy with the smallest `ingest_ts` wins, and the other copies are marked dropped in place. That holds even when the winning copy is invalid.
5. **Sessions and attribution.** In each person partition, records are grouped by person with a counting sort and sorted by `ts`. Sessionizing uses the 30-minute gap. For each session, a sliding 7-day window over the person's clicks picks the credited click: the latest `ts`, with ties going to the smallest `click_id`. The same window gives the distinct assisting campaigns.
6. **Write.** A hand-written Parquet writer outputs PLAIN-encoded, uncompressed files, one per thread for `user_daily`. `day` is an INT32 with the DATE type. The process exits without freeing its memory.

On the 20M-event set, a laptop run takes about 1.0 s with a peak of about 1.4 GB of memory, against 1.4 s and 4.8 GB for the C++ entry.

## Build and run

```sh
docker buildx build --platform linux/amd64 --load -t shopping-sessions:latest .
docker run --rm -v "$PWD/dev:/data:ro" -v "$PWD/out:/output" -e NCPU=8 shopping-sessions:latest
```

To build outside Docker:

```sh
RUSTFLAGS="-C target-cpu=native" cargo build --release
DATA_DIR=dev OUTPUT_DIR=out target/release/pipeline     # or: target/release/pipeline DATA_DIR OUTPUT_DIR
```

Set `PROFILE=1` to print per-phase timings to stderr.

The C++ entry builds from its own folder: `docker buildx build --platform linux/amd64 --load -t shopping-sessions:cpp cpp`.
