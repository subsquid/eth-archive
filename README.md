# eth-archive

## Usage

#### Install cargo make

`cargo install --force cargo-make`

#### Build and run ingester

`makers ingester`

#### Build and run worker

`makers worker`

#### Run MinIO Client to sync to s3 compatible storage

[Link to minio docs](https://min.io/docs/minio/linux/reference/minio-mc.html#quickstart)

## Development

Runtime arguments can be changed by editing `Makefile.toml` in project root.

Please run `cargo fmt` and `cargo clippy` before pushing.
