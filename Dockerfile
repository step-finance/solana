FROM rust:1.69-buster as base
RUN apt-get update
RUN apt-get install libssl-dev libudev-dev pkg-config zlib1g-dev llvm clang cmake make libprotobuf-dev protobuf-compiler

FROM base as builder
WORKDIR /solana
COPY . .
RUN cargo build --release

FROM base as release
COPY --from=builder /solana/target/release /solana
