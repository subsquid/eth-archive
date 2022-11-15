# eth-archive

## Usage

#### Install cargo make

- `cargo install --force cargo-make`

#### Build and run ingester

- `makers ingester`

#### Build and run worker

- `makers worker`

- configure `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`
- `makers ingester-s3`
- `makers worker-s3`
