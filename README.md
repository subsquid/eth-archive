# eth-archive

## Usage

Note: Parquet writer and gateway need to be run on same machine until remote storage is implemented.

All binaries can be started like `cargo run --release --bin eth-archive-<ingester/parquet-writer/gateway>` from the project root.

All binaries can be configured by editing the toml files found in project root. They also take a command line argument to specify config file path (`--cfg-path`).

Ingester takes a `--reset-data` cli argument which makes it reset the postgres database while starting.

Parquet writer takes a `--reset-data` cli argument which makes it delete all of the parquet files while starting.

All binaries should be restart-able from where they left of if the process crashes.

Only needed configuration change would be `db` section of the config files. Which configures the postgres connection. Also maybe path parameters like `logs_path` of gateway and `block.path` of parquet writer.

Cli arguments can be passed like `cargo run --release --bin eth-archive-<ingester/parquet-writer-gateway> -- --reset-data --cfg-path=/some/path`

Need to set `RUST_LOG=info` to see logs in console.

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
Query can terminate due to;

- maximum query time limit being reached
- maximum number of logs being found
- scanning up to the target block

Client can continue querying using the `nextBlock` field in the response

request:

```javascript
{
  "fromBlock": number, // starting block number to include in range
  "toBlock": Option<number>, // ending block number of the range. returned block range is [fromBlock, toBlock]. So toBlock is included.
  "logs": [{
    "address": Option<[string]>, // contract addresses to filter by. all addresses are included if null 
    // if topics[0] is ["a", "b", "c"] the logs will be filtered so only logs that have "a", "b" or "c" as their first topic will be returned.
    // if topics[0] is an empty array, any topic will pass the filter
    "topics": [[string]],
    "fieldSelection": FieldSelection
  }],
}
```
[FieldSelection](https://github.com/subsquid/eth-archive/blob/master/gateway/src/field_selection.rs)

response:

```javascript
{
  "data": [[{
    "block": BlockData,
    "transactions": [TransacitonData],
    "logs": [LogData],
  }]],
  "metrics": {
    "buildQuery": number, // milliseconds it took to build the query
    "runQuery": number, // milliseconds it took to run the query
    "serializeResult": number, // milliseconds it took to serialize the results to common types (doesn't include json serialization time) 
    "total": number, // total number of milliseconds (doesn't include json serialization time)
  },
  "status": {
    "parquetBlockNumber": number, // max block number in the parquet storage
    "dbMaxBlockNumber": number, // max block number in hot storage
    "dbMinBlockNumber": number, // min block number in hot storage
  },
  "nextBlock": number, // next block number to query from
}

```

</details>

## Testing

Tests can be executed by the following command: `cargo test`.
In order to update [tests fixtures](gateway/tests/data) you should update [data.json](scripts/generate-parquets/data.json) and run `scripts/generate-parquets.sh`.
