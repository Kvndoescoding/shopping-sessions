# Shopping Sessions: native C++ entry

A Builders Buildathon entry for the Shopping Sessions track. It turns the click-stream CSVs in `/data` into the two daily Parquet tables, `/output/user_daily/` and `/output/campaign_daily/`, following the track's `rules.md`.

The whole pipeline is one C++17 file, `pipeline.cpp`, with no dependencies. The `Dockerfile` compiles it into one static binary.

## How it works

1. **Load.** Every CSV is memory-mapped and split into 4 MB chunks. Each chunk starts at a line boundary, and each file's header line is skipped.
2. **Parse.** AVX2 finds commas, quotes and newlines 64 bytes at a time. A field is either bare or wrapped in quotes as a whole; any other line is malformed and dropped. Timestamps (forms F1, F2 and F3) and prices are checked by hand.
3. **Links.** Linked IDs are bucketed by hash, interned per bucket, and joined with union-find. Each person is named by the byte-wise smallest of their IDs. This runs on a background thread while events and clicks parse.
4. **Dedup.** Each event line emits a small key: its `event_id` hash, its `ingest_ts`, and where the record lives. Keys are bucketed by hash. The copy with the smallest `ingest_ts` wins, and the other copies are marked dead in place. That holds even when the winning copy is invalid.
5. **Resolve and regroup.** Users are mapped to persons through cache-sized tables, one per bucket. Records are then scattered into one array per person bucket, using counts from a first pass.
6. **Sessions and attribution.** In each person bucket, records are grouped by person and sorted by `ts`. Sessionizing uses the 30-minute gap. For each session, a sliding 7-day window over the person's clicks picks the credited click: the latest `ts`, with ties going to the smallest `click_id`. The same window gives the distinct assisting campaigns.
7. **Write.** A minimal Parquet writer outputs PLAIN-encoded, uncompressed files, one per thread for `user_daily`. `day` is an INT32 with the DATE type.

## Build and run

```sh
docker buildx build --platform linux/amd64 --load -t shopping-sessions:latest .
docker run --rm -v "$PWD/dev:/data:ro" -v "$PWD/out:/output" -e NCPU=8 shopping-sessions:latest
```

To build outside Docker:

```sh
g++ -O3 -march=native -std=c++17 -pthread -o pipeline pipeline.cpp
DATA_DIR=dev OUTPUT_DIR=out ./pipeline        # or: ./pipeline DATA_DIR OUTPUT_DIR
```

Set `PROFILE=1` to print per-phase timings to stderr.
