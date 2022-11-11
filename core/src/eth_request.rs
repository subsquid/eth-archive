use crate::types::{Block, Log};
use serde::de::DeserializeOwned;
use serde_json::Value as JsonValue;

pub trait EthRequest {
    type Resp: DeserializeOwned;

    fn to_body(&self, id: usize) -> JsonValue;
}

#[derive(Debug, Clone, Copy)]
pub struct GetBlockByNumber {
    pub block_number: u32,
}

impl EthRequest for GetBlockByNumber {
    type Resp = Block;

    fn to_body(&self, id: usize) -> JsonValue {
        serde_json::json!({
            "jsonrpc": "2.0",
            "method": "eth_getBlockByNumber",
            "params": [
                block_number_to_hex(self.block_number),
                true,
            ],
            "id": id,
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct GetLogs {
    pub from_block: u32,
    pub to_block: u32,
}

impl EthRequest for GetLogs {
    type Resp = Vec<Log>;

    fn to_body(&self, id: usize) -> JsonValue {
        serde_json::json!({
            "jsonrpc": "2.0",
            "method": "eth_getLogs",
            "params": [
                {
                    "fromBlock": block_number_to_hex(self.from_block),
                    "toBlock": block_number_to_hex(self.to_block),
                }
            ],
            "id": id,
        })
    }
}

#[derive(Clone, Copy)]
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

fn block_number_to_hex(block_number: u32) -> String {
    format!("0x{:x}", block_number)
}
