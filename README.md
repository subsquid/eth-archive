# eth-archive

## Usage

Note: Parquet writer and gateway need to be run on same machine until remote storage is implemented.

All binaries can be started like `cargo run --release --bin eth-archive-<ingester/parquet-writer/gateway>` from the project root.

The machine that will run gateway and parquet writer needs to have about 500GB of storage. It also needs to have 16GB of ram.

All binaries can be configured by editing the toml files found in project root. They also take a command line argument to specify config file path (`--cfg-path`).

Ingester takes a `--reset-data` cli argument which makes it reset the postgres database while starting.

Parquet writer takes a `--reset-data` cli argument which makes it delete all of the parquet files while starting.

All binaries should be restart-able from where they left of if the process crashes.

Only needed configuration change would be `db` section of the config files. Which configures the postgres connection. Also maybe path parameters like `logs_path` of gateway and `block.path` of parquet writer.

Cli arguments can be passed like `cargo run --release --bin eth-archive-<ingester/parquet-writer-gateway> -- --reset-data --cfg-path=/some/path`

All components use rust env_logger so setting `RUST_LOG` env variable to `info` is necessary to see info logs on console


## Gateway API

<details>
<summary>GET /status</summary>
response:

```javascript
{
  "parquetBlockNumber": number, // max block number in the parquet storage
  "dbMaxBlockNumber": number, // max block number in hot storage
  "dbMinBlockNumber": number, // min block number in hot storage
}
```

</details>

<details>
<summary>POST /query</summary>
request:

```javascript
{
  "fromBlock": number, // starting block number to include in range
  "toBlock": number, // ending block number of the range. returned block range is [fromBlock, toBlock). So toBlock is not included.
  "addresses": [{
    "address": string, // address of the contract
    // there has to be four entries, each entry is either null or a list of topics which will be used to filter.
    // if topics[0] is ["a", "b", "c"] the logs will be filtered so only logs that have "a", "b" or "c" as their first topic will be returned.
    "topics": [null || [string]] 
  }],
  "fieldSelection": FieldSelection
}
```

[FieldSelection](https://github.com/subsquid/eth-archive/blob/21376a8a92c993c10376bc992f1d0627ec3e9f09/gateway/src/field_selection.rs#L30)

response:

```javascript
{
  "data": [ResponseRow],
  "metrics": {
    "buildQuery": number, // milliseconds it took to build the query
    "runQuery": number, // milliseconds it took to run the query
    "serializeResult": number, // milliseconds it took to serialize the results to common types (doesn't include json serialization time) 
    "total": number, // total number of milliseconds (doesn't include json serialization time)
  }
}

```

[ResponseRow](https://github.com/subsquid/eth-archive/blob/21376a8a92c993c10376bc992f1d0627ec3e9f09/core/src/types.rs#L185)

</details>
