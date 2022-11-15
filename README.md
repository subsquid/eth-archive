# eth-archive

## Usage

#### Install cargo make

- `cargo install --force cargo-make`

#### Build and run ingester

- `makers ingester`

#### Build and run worker

- `makers worker`

#### Build and run with s3

- configure `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`
- `makers ingester-s3`
- `makers worker-s3`

## Development

Runtime arguments can be changed by editing `Makefile.toml` in project root.

Please run `cargo fmt` and `cargo clippy` before pushing.
