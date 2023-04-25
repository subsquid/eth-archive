# eth-archive

## Usage

### Requirements

- Latest stable version of Rust. Code is only tested and built using the latest stable version of rustc.

#### Install cargo make

- `cargo install --force cargo-make`

#### Build and run ingester

- `makers ingester`

#### Build and run worker

- `makers worker`

#### Run with s3
- configure `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`
- `makers ingester-s3`
- `makers worker-s3`

#### Helm charts

Reference Helm charts for the worker and the ingester services can be found in the `charts` folder.



#### CLI
<details>

<summary>Ingester</summary>
  
```
Usage: eth-archive-ingester.exe [OPTIONS] --data-path <DATA_PATH> --request-timeout-secs <REQUEST_TIMEOUT_SECS> --connect-timeout-ms <CONNECT_TIMEOUT_MS> --block-batch-size <BLOCK_BATCH_SIZE> --http-req-concurrency <HTTP_REQ_CONCURRENCY> --best-block-offset <BEST_BLOCK_OFFSET> --max-blocks-per-file <MAX_BLOCKS_PER_FILE> --max-txs-per-file <MAX_TXS_PER_FILE> --max-logs-per-file <MAX_LOGS_PER_FILE> --max-row-groups-per-file <MAX_ROW_GROUPS_PER_FILE>

Options:
      --data-path <DATA_PATH>
          Path to store parquet files
      --request-timeout-secs <REQUEST_TIMEOUT_SECS>
          Http request timeout in seconds
      --connect-timeout-ms <CONNECT_TIMEOUT_MS>
          Http connect timeout in milliseconds
      --block-batch-size <BLOCK_BATCH_SIZE>
          Number of blocks a single get block batch request will cover
      --http-req-concurrency <HTTP_REQ_CONCURRENCY>
          Number of concurrent requests to make
      --best-block-offset <BEST_BLOCK_OFFSET>
          Offset from tip of the chain (to avoid rollbacks)
      --rpc-urls <RPC_URLS>

      --get-receipts
          Get transaction receipts. This fills the transaction.status field. Requires eth_getBlockReceipts to be available on the RPC API
      --wait-between-rounds <WAIT_BETWEEN_ROUNDS>
          Wait this amount of seconds between rounds
      --max-rpc-endpoint-best-block-diff <MAX_RPC_ENDPOINT_BEST_BLOCK_DIFF>
          An rpc endpoint is considered behind and excluded if max_best_block - rpc_endpoint.best_block > max_rpc_endpoint_best_block_diff [default: 5]
      --target-rpc-endpoint <TARGET_RPC_ENDPOINT>
          The real target rpc endpoint. This is useful when using the rpc_proxy
      --num-tries <NUM_TRIES>

      --secs-between-tries <SECS_BETWEEN_TRIES>
          [default: 3]
      --max-blocks-per-file <MAX_BLOCKS_PER_FILE>
          Maximum number of blocks per parquet file
      --max-txs-per-file <MAX_TXS_PER_FILE>
          Maximum number of transactions per file
      --max-logs-per-file <MAX_LOGS_PER_FILE>
          Maximum number of logs per parquet file
      --max-row-groups-per-file <MAX_ROW_GROUPS_PER_FILE>
          Maximum number of row groups per parquet file
      --parquet-page-size <PARQUET_PAGE_SIZE>
          Page size for parquet files in bytes. Defaults to 1MB
      --max-pending-folder-writes <MAX_PENDING_FOLDER_WRITES>
          Maximum number of pending folder writes. This effects maximum memory consumption [default: 8]
      --folder-write-concurrency <FOLDER_WRITE_CONCURRENCY>
          [default: 8]
      --metrics-addr <METRICS_ADDR>
          Address to serve prometheus metrics from [default: 127.0.0.1:8181]
      --s3-src-bucket <S3_SRC_BUCKET>
          S3 bucket name to initial sync from
      --s3-src-format-ver <S3_SRC_FORMAT_VER>
          Source data format version
      --local-src-path <LOCAL_SRC_PATH>
          Local file system path to sync from
      --local-src-format-ver <LOCAL_SRC_FORMAT_VER>
          Local source data format version
      --s3-endpoint <S3_ENDPOINT>

      --s3-bucket-name <S3_BUCKET_NAME>

      --s3-bucket-region <S3_BUCKET_REGION>

      --s3-sync-interval-secs <S3_SYNC_INTERVAL_SECS>

      --s3-concurrency <S3_CONCURRENCY>
```
  
</details>
 
<details>

<summary>Worker</summary>
  
```
Usage: eth-archive-worker.exe [OPTIONS] --db-path <DB_PATH> --request-timeout-secs <REQUEST_TIMEOUT_SECS> --connect-timeout-ms <CONNECT_TIMEOUT_MS> --block-batch-size <BLOCK_BATCH_SIZE> --http-req-concurrency <HTTP_REQ_CONCURRENCY> --best-block-offset <BEST_BLOCK_OFFSET> --max-resp-body-size <MAX_RESP_BODY_SIZE> --resp-time-limit <RESP_TIME_LIMIT>

Options:
      --db-path <DB_PATH>
          Database path
      --data-path <DATA_PATH>
          Path to read parquet files from
      --request-timeout-secs <REQUEST_TIMEOUT_SECS>
          Http request timeout in seconds
      --connect-timeout-ms <CONNECT_TIMEOUT_MS>
          Http connect timeout in milliseconds
      --block-batch-size <BLOCK_BATCH_SIZE>
          Number of blocks a single get block batch request will cover
      --http-req-concurrency <HTTP_REQ_CONCURRENCY>
          Number of concurrent requests to make
      --best-block-offset <BEST_BLOCK_OFFSET>
          Offset from tip of the chain (to avoid rollbacks)
      --rpc-urls <RPC_URLS>

      --get-receipts
          Get transaction receipts. This fills the transaction.status field. Requires eth_getBlockReceipts to be available on the RPC API
      --wait-between-rounds <WAIT_BETWEEN_ROUNDS>
          Wait this amount of seconds between rounds
      --max-rpc-endpoint-best-block-diff <MAX_RPC_ENDPOINT_BEST_BLOCK_DIFF>
          An rpc endpoint is considered behind and excluded if max_best_block - rpc_endpoint.best_block > max_rpc_endpoint_best_block_diff [default: 5]
      --target-rpc-endpoint <TARGET_RPC_ENDPOINT>
          The real target rpc endpoint. This is useful when using the rpc_proxy
      --num-tries <NUM_TRIES>

      --secs-between-tries <SECS_BETWEEN_TRIES>
          [default: 3]
      --server-addr <SERVER_ADDR>
          Address to be used for running the server [default: 127.0.0.1:8080]
      --initial-hot-block-range <INITIAL_HOT_BLOCK_RANGE>
          Initial hot block range. If None, hot blocks will start from 0
      --max-resp-body-size <MAX_RESP_BODY_SIZE>
          Query stops as soon as the response body size in megabytes reaches this number. Response body might be bigger than this amount of MBs
      --max-concurrent-queries <MAX_CONCURRENT_QUERIES>
          Maximum number of concurrent queries [default: 32]
      --max-parquet-query-concurrency <MAX_PARQUET_QUERY_CONCURRENCY>
          Maximum number of threads per query to use to query parquet folders [default: 8]
      --resp-time-limit <RESP_TIME_LIMIT>
          Response time limit in milliseconds. The query will stop and found data will be returned if the request takes more than this amount of time to handle
      --db-query-batch-size <DB_QUERY_BATCH_SIZE>
          Size of each database query. Database queries are batched because we don't want to query the entire db at once [default: 200]
      --s3-endpoint <S3_ENDPOINT>

      --s3-bucket-name <S3_BUCKET_NAME>

      --s3-bucket-region <S3_BUCKET_REGION>

      --s3-sync-interval-secs <S3_SYNC_INTERVAL_SECS>

      --s3-concurrency <S3_CONCURRENCY>
```
  
</details>

## Architecture

<img src="https://user-images.githubusercontent.com/8627422/225257301-a1ff18bb-57ee-4e7a-960e-62ac83afda7d.png" width="75%">

## API Docs


<details>

<summary><code>GET</code> <code><b>/height</b></code> <code>(get height of the archive)</code></summary>

##### Example Response

```json
{
  "height": 16576911
}
```

</details>

<details>

<summary><code>POST</code> <code><b>/query</b></code> <code>(query logs and transactions)</code></summary>

##### Query Fields

- **fromBlock**: Block number to start from (inclusive).
- **toBlock**: Block number to end on (inclusive) (optional). If this is not given, the query will go on for a fixed amount of time or until it reaches the height of the archive.
- **logs.address**: Array of addresses to query for. A log will be included in the response if the log's address matches any of the addresses given in the query. (null or empty array means any address).
- **log.topics**: Array of arrays of topics. Outer array has an element for each topic an EVM log can have. Each inner array represents possible matching values for a topic. For example topics[2] is an array of possible values that should match the log's third topic or the log won't be included in the response. Empty arrays match everything.
- **transactions.from** and **transactions.to**: Array of addresses that should match the transaction's `to` field or the transaction's `from`. If none of these match, the transaction won't be included in the response. If both are null or empty array, any address will pass.
- **transactions.sighash**: Array of values that should match first four bytes of the transaction input. null or empty array means any value will pass.

<details>

<summary>

##### Example Request
</summary>

```json
{
  "fromBlock": 14495889,
  "toBlock": 14495889,
  "logs": [
    {
      "address": [
        "0x3883f5e181fccaF8410FA61e12b59BAd963fb645"
      ],
      "topics": [
        [
          "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
        ]
      ],
      "fieldSelection": {
        "block": {
          "parentHash": true,
          "sha3Uncles": true,
          "miner": true,
          "stateRoot": true,
          "transactionsRoot": true,
          "receiptsRoot": true,
          "logsBloom": true,
          "difficulty": true,
          "number": true,
          "gasLimit": true,
          "gasUsed": true,
          "timestamp": true,
          "extraData": true,
          "mixHash": true,
          "nonce": true,
          "totalDifficulty": true,
          "baseFeePerGas": true,
          "size": true,
          "hash": true
        },
        "transaction": {
          "type": true,
          "nonce": true,
          "to": true,
          "gas": true,
          "value": true,
          "input": true,
          "maxPriorityFeePerGas": true,
          "maxFeePerGas": true,
          "yParity": true,
          "chainId": true,
          "v": true,
          "r": true,
          "s": true,
          "from": true,
          "blockHash": true,
          "blockNumber": true,
          "index": true,
          "gasPrice": true,
          "hash": true,
          "status": true
        },
        "log": {
          "address": true,
          "blockHash": true,
          "blockNumber": true,
          "data": true,
          "index": true,
          "removed": true,
          "topics": true,
          "transactionHash": true,
          "transactionIndex": true
        }
      }
    }
  ],
  "transactions": [
    {
      "address": [
        "0x3883f5e181fccaf8410fa61e12b59bad963fb645"
      ],
      "sighash": [
        "0xa9059cbb"
      ],
      "fieldSelection": {
        "block": {
          "parentHash": true,
          "sha3Uncles": true,
          "miner": true,
          "stateRoot": true,
          "transactionsRoot": true,
          "receiptsRoot": true,
          "logsBloom": true,
          "difficulty": true,
          "number": true,
          "gasLimit": true,
          "gasUsed": true,
          "timestamp": true,
          "extraData": true,
          "mixHash": true,
          "nonce": true,
          "totalDifficulty": true,
          "baseFeePerGas": true,
          "size": true,
          "hash": true
        },
        "transaction": {
          "type": true,
          "nonce": true,
          "to": true,
          "gas": true,
          "value": true,
          "input": true,
          "maxPriorityFeePerGas": true,
          "maxFeePerGas": true,
          "yParity": true,
          "chainId": true,
          "v": true,
          "r": true,
          "s": true,
          "from": true,
          "blockHash": true,
          "blockNumber": true,
          "index": true,
          "gasPrice": true,
          "hash": true,
          "status": true
        },
        "log": {
          "address": true,
          "blockHash": true,
          "blockNumber": true,
          "data": true,
          "index": true,
          "removed": true,
          "topics": true,
          "transactionHash": true,
          "transactionIndex": true
        }
      }
    }
  ]
}
```

</details>

<details>

<summary>

##### Example Response
</summary>

```json
{
  "data": [
    [
      {
        "block": {
          "parentHash": "0x455864413159d92478ad496a627533ce6fdd83d6ed47528b8790b96135325d64",
          "sha3Uncles": "0x9098605a7fc9c4c70622f6458f168b9acb302e6f84d02235670b49e263a3bd13",
          "miner": "0xea674fdde714fd979de3edf0f56aa9716b898ec8",
          "stateRoot": "0xfb8b174c4e118d95ff5097014ed1d60c8b49094b541b1e9e4e16198963ddbdf6",
          "transactionsRoot": "0xb8eebad6f727be7afa3d6812ff2c05adb1e3053ac15bfb6acdd0265c8ffd2cce",
          "receiptsRoot": "0x4fa02eab7297cfbadd39d3e47cdf52f85159f4b9719621c2ec6a9b608638a482",
          "logsBloom": "0xb43fd1cee1dfdf4b7cbef44ced7f9ebe70c8e7ddfe764cb84ba96beb352ff49d751f196fd0b87fcf3d3e5a2241f56f714a1280c30be3ed27ef7f9f7bf0b7aa7d94de4fe8ea476fea6f7364ed917bd3e3af3f7a1ec3d4f910fee067a6cabffb7f7eaadbe70e5e8a6ec4ecf6f8959b7adbec62fdbc8513855c66ea15fbacca1b64517c8f94e766be97b4fefdd8a5952ef45ae56ffbc9cad78948b32ddf6ed4b83333fdee65b97d6897fe9fe8f9ed583c4c4a90e2cbe9d5bb9f2375932c77bddd77bf08ffa3df263f777de5228103bff47b8d6df97fa79fce9b0c7a3fea0f43e73f143c388f5d732b9cb9e5fe889bd04ff31b4b84e1ffe407d222ffce85f8567aeb",
          "difficulty": "0x2e878564548bfa",
          "number": 14495889,
          "gasLimit": "0x01ca35ef",
          "gasUsed": "0x01c9e9fe",
          "timestamp": "0x6246042d",
          "extraData": "0x617369612d65617374322d3134",
          "mixHash": "0xbfcdb3683cbefbfe178e0334acf34005cd20dc06440f311cd9270236b6dea952",
          "nonce": "17265704492997010732",
          "totalDifficulty": "0x0991caa08ff39c6e95a4",
          "baseFeePerGas": "0x0c97b03dcf",
          "size": "0x023860",
          "hash": "0x344fc5e67555bfb42f759be7ee372fad30bcbebf780cbfb901f546683ed22517"
        },
        "transactions": [
          {
            "type": 2,
            "nonce": "6",
            "to": "0x3883f5e181fccaf8410fa61e12b59bad963fb645",
            "gas": "0xd3bb",
            "value": "0x00",
            "input": "0xa9059cbb000000000000000000000000254d3f04543145f3a991b61675ca0bb353f669c90000000000000000000000000000000000000000000000008ac7230489e80000",
            "maxPriorityFeePerGas": "0x4a817c80",
            "maxFeePerGas": "0x0cff79e223",
            "chainId": 1,
            "v": "0",
            "r": "0xbd768420f1c173f6942d201b83bc7aedef90d4b9947598a2e931525560b722ed",
            "s": "0x78387f73fd4b6cd006eaf8deeda0857234e29740c06e01221460c7118daef348",
            "from": "0x5bf2f6612dfc3d0d1e0c6799534228b41369d39e",
            "blockHash": "0x344fc5e67555bfb42f759be7ee372fad30bcbebf780cbfb901f546683ed22517",
            "blockNumber": 14495889,
            "index": 299,
            "gasPrice": "0x0ce231ba4f",
            "hash": "0x8f45965dd61dc189b94306b0f13cc3338374f687d527c64c1f19c994b39ae3b2"
          }
        ],
        "logs": [
          {
            "address": "0x3883f5e181fccaf8410fa61e12b59bad963fb645",
            "blockHash": "0x344fc5e67555bfb42f759be7ee372fad30bcbebf780cbfb901f546683ed22517",
            "blockNumber": 14495889,
            "data": "0x0000000000000000000000000000000000000000000000008ac7230489e80000",
            "index": 379,
            "removed": false,
            "topics": [
              "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
              "0x0000000000000000000000005bf2f6612dfc3d0d1e0c6799534228b41369d39e",
              "0x000000000000000000000000254d3f04543145f3a991b61675ca0bb353f669c9"
            ],
            "transactionHash": "0x8f45965dd61dc189b94306b0f13cc3338374f687d527c64c1f19c994b39ae3b2",
            "transactionIndex": 299
          }
        ]
      }
    ]
  ],
  "archiveHeight": 16577057,
  "nextBlock": 14495890,
  "totalTime": 220
}
```
</details>

</details>
