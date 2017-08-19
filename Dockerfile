# Full TemporalStore (Rust) image — every service binary in one image.
#
#   docker build -f Dockerfile -t temporalstore:full .
#   docker run --rm -p 17101:17101 -p 17102:17102 temporalstore:full
#
# By default this runs a single node (metaserver + datanode), exactly like
# docker/Dockerfile.single-node, but the image also ships the proxy, service
# proxy, direct SDK, and native persistence workflow binaries for advanced use.
# For the smallest/fastest image, use docker/Dockerfile.single-node instead.
FROM rust:1.87-bookworm AS builder
# librocksdb-sys (pulled in by matrixcache's `rocksdb-ssd` feature) runs bindgen
# at build time and needs libclang; building RocksDB itself needs cmake.
RUN apt-get update \
    && apt-get install -y --no-install-recommends clang libclang-dev cmake \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY proto ./proto
RUN cargo build --release -p temporalstore-rust

FROM debian:bookworm-slim
RUN apt-get update \
