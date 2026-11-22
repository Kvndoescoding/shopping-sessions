# Shopping Sessions: native C++ pipeline.
# Build from this folder: docker buildx build --platform linux/amd64 --load -t shopping-sessions:cpp cpp
# Build stage compiles one static binary; the run stage only holds that binary.
FROM debian:bookworm-slim AS build
RUN apt-get update \
 && apt-get install -y --no-install-recommends g++ libc6-dev \
 && rm -rf /var/lib/apt/lists/*
COPY pipeline.cpp /src/pipeline.cpp
# Scorers are Cascade Lake or newer: AVX2/BMI2 are safe, tune for Cascade Lake.
RUN g++ -O3 -march=x86-64-v3 -mtune=cascadelake -std=c++17 -pthread -static \
        -o /pipeline /src/pipeline.cpp

FROM debian:bookworm-slim
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY --from=build /pipeline /app/pipeline
WORKDIR /app
# Reads DATA_DIR / OUTPUT_DIR / NCPU from the environment (defaults /data, /output).
CMD ["/app/pipeline"]
