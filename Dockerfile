FROM rustlang/rust:nightly-bullseye-slim AS builder
ARG component
RUN apt-get update && apt-get upgrade -y && apt-get -y install build-essential llvm clang cmake liburing1 liburing-dev curl pkg-config
WORKDIR /eth
COPY ./ .
RUN cargo build --features rocksdb-unix --release --bin "eth-archive-${component}"

FROM debian:bullseye
ARG component
WORKDIR /eth
RUN apt-get update && apt-get upgrade -y && apt-get install liburing1 ca-certificates -y
COPY --from=builder "/eth/target/release/eth-archive-${component}" "./eth-archive-${component}"
CMD ["/eth/${component}"]
