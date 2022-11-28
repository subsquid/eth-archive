FROM ubuntu:impish AS builder
ARG component
RUN apt-get update && apt-get upgrade -y && apt-get -y install build-essential llvm clang cmake liburing2 liburing-dev curl
RUN curl https://sh.rustup.rs -sSf | bash -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"
WORKDIR /eth
COPY ./ .
RUN cargo build --release --bin "eth-archive-${component}"

FROM ubuntu:impish
ARG component
WORKDIR /eth
RUN apt-get update && apt-get upgrade -y && apt-get install liburing2 ca-certificates -y
COPY --from=builder "/eth/target/release/eth-archive-${component}" "./eth-archive-${component}"
CMD ["/eth/${component}"]
