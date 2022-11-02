FROM rust:1.64.0-bullseye AS builder
ARG component
RUN apt-get update && apt-get upgrade -y && apt-get -y install build-essential llvm clang cmake
WORKDIR /eth
COPY ./ .
RUN cargo build --release --bin "eth-archive-${component}"

FROM buildpack-deps:bullseye-curl
ARG component
WORKDIR /eth
RUN apt-get update && apt-get upgrade -y && apt-get -y install libatomic1
COPY --from=builder "/eth/target/release/eth-archive-${component}" "./eth-archive-${component}"
CMD ["/eth/${component}"]
