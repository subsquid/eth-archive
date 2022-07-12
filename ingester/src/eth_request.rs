use crate::schema::{Block, Log};
use serde::de::DeserializeOwned;
use serde_json::Value as JsonValue;

pub trait EthRequest {
    type Resp: DeserializeOwned;

    fn to_body(&self, id: usize) -> JsonValue;
}

#[derive(Debug, Clone, Copy)]
pub struct GetBlockByNumber {
    pub block_number: usize,
}

impl EthRequest for GetBlockByNumber {
    type Resp = Block;

    fn to_body(&self, id: usize) -> JsonValue {
        serde_json::json!({
            "jsonrpc": "2.0",
            "method": "eth_getBlockByNumber",
            "params": [
                hex::encode(self.block_number),
                true,
            ],
            "id": id,
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct GetLogs {
    pub from_block: usize,
    pub to_block: usize,
}

impl EthRequest for GetLogs {
    type Resp = Vec<Log>;

    fn to_body(&self, id: usize) -> JsonValue {
        serde_json::json!({
            "jsonrpc": "2.0",
            "method": "eth_getLogs",
            "params": [
                {
                    "fromBlock": hex::encode(self.from_block),
                    "toBlock": hex::encode(self.to_block),
                }
            ],
            "id": id,
        })
    }
}

pub struct GetBestBlock {}

impl EthRequest for GetBestBlock {
    type Resp = String;

    fn to_body(&self, id: usize) -> JsonValue {
        serde_json::json!({
            "jsonrpc": "2.0",
            "method": "eth_blockNumber",
            "params": [],
            "id": id,
        })
    }
}
