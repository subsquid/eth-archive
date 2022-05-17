use crate::schema::{Block, Log};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

pub trait EthRequest {
    type Resp: DeserializeOwned;

    fn to_body(&self, id: usize) -> JsonValue;
}

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
                block_number_to_hex(self.block_number),
                true,
            ],
            "id": id,
        })
    }
}

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
                    "from_block": block_number_to_hex(self.from_block),
                    "to_block": block_number_to_hex(self.to_block),
                }
            ],
            "id": id,
        })
    }
}

fn block_number_to_hex(block_number: usize) -> String {
    format!("0x{:x}", block_number)
}
