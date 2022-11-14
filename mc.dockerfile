FROM ubuntu:latest
RUN apt-get update && apt-get upgrade -y && apt-get -y install curl
RUN curl https://dl.min.io/client/mc/release/linux-amd64/mc \
      --create-dirs \
      -o $HOME/minio-binaries/mc && \
    chmod +x $HOME/minio-binaries/mc && \
    mv $HOME/minio-binaries/mc /usr/local/bin

