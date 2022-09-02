FROM rustlang/rust:nightly-bullseye AS builder
ARG component
RUN apt-get update && apt-get upgrade -y && apt-get -y install build-essential llvm clang cmake
WORKDIR /eth
COPY ./ .
ENV RUSTFLAGS="-C target-cpu=native"
RUN cargo build --release --bin "eth-archive-${component}"

FROM debian:bullseye-slim
ARG component
WORKDIR /eth
RUN apt-get update && apt-get upgrade -y && apt-get -y install libatomic1
COPY --from=builder "/eth/target/release/eth-archive-${component}" "./eth-archive-${component}"
CMD ["/eth/${component}"]
