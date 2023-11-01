FROM rust:1.69-buster as base
RUN apt-get update
RUN apt-get install -y libssl-dev libudev-dev pkg-config zlib1g-dev llvm clang cmake make libprotobuf-dev protobuf-compiler

FROM base as builder
WORKDIR /solana
COPY . .
RUN cargo build --release
RUN cp -r ./target/release/* /usr/local/bin
WORKDIR /
RUN rm -r solana

# Want this line? Can run a quick instance with `docker run`
CMD ["solana-test-validator"]