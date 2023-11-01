FROM rust:1.69-buster
RUN apt-get update
RUN apt-get install libssl-dev libudev-dev pkg-config zlib1g-dev llvm clang cmake make libprotobuf-dev protobuf-compiler
COPY . .
RUN cargo build --release
