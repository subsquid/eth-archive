# eth-archive

## Usage

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
          "id": true,
          "number": true,
          "hash": true,
          "parentHash": true,
          "nonce": true
        },
        "log": {
          "id": true,
          "address": true,
          "index": true,
          "transactionIndex": true,
          "topics": true,
          "data": true
        },
        "transaction": {
          "id": true,
          "to": true,
          "index": true,
          "hash": true
        }
      }
    }
  ],
  "transactions": []
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
          "number": 14495889,
          "nonce": "17265704492997010732",
          "hash": "0x344fc5e67555bfb42f759be7ee372fad30bcbebf780cbfb901f546683ed22517"
        },
        "transactions": [
          {
            "to": "0x3883f5e181fccaf8410fa61e12b59bad963fb645",
            "blockNumber": 14495889,
            "index": 299,
            "hash": "0x8f45965dd61dc189b94306b0f13cc3338374f687d527c64c1f19c994b39ae3b2"
          }
        ],
        "logs": [
          {
            "address": "0x3883f5e181fccaf8410fa61e12b59bad963fb645",
            "blockNumber": 14495889,
            "data": "0x0000000000000000000000000000000000000000000000008ac7230489e80000",
            "index": 379,
            "topics": [
              "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
              "0x0000000000000000000000005bf2f6612dfc3d0d1e0c6799534228b41369d39e",
              "0x000000000000000000000000254d3f04543145f3a991b61675ca0bb353f669c9"
            ],
            "transactionIndex": 299
          }
        ]
      }
    ]
  ],
  "archiveHeight": 16576932,
  "nextBlock": 14495890,
  "totalTime": 58
}
```

</details>
</details>
