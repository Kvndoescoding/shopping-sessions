# Shopping Sessions: native Rust pipeline.
# Build stage compiles one binary; the run stage only holds that binary.
# Build from this folder:  docker build -t shopping-sessions-rs .
FROM rust:1.100.0-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src/main.rs src/main.rs
# Scorers are Cascade Lake or newer: AVX2/BMI2 are safe (same level as the C++ build).
ENV RUSTFLAGS="-C target-cpu=x86-64-v3"
RUN cargo build --release --locked \
 && cp target/release/pipeline /pipeline

FROM debian:bookworm-slim
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY --from=build /pipeline /app/pipeline
WORKDIR /app
# Reads DATA_DIR / OUTPUT_DIR / NCPU from the environment (defaults /data, /output).
CMD ["/app/pipeline"]
