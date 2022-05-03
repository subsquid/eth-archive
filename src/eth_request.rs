use parquet::record::RecordWriter;
use parquet_derive::ParquetRecordWriter;
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

fn block_number_to_hex(block_number: usize) -> String {
    format!("0x{:x}", block_number)
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Block {
    pub difficulty: String,
    pub extra_data: String,
    pub gas_limit: String,
    pub gas_used: String,
    pub hash: String,
    pub logs_bloom: String,
    pub miner: String,
    pub mix_hash: String,
    pub nonce: String,
    pub number: String,
    pub parent_hash: String,
    pub receipts_root: String,
    pub sha3_uncles: String,
    pub size: String,
    pub state_root: String,
    pub timestamp: String,
    pub total_difficulty: String,
    pub transactions: Vec<Transaction>,
    pub transactions_root: String,
}

#[derive(Debug, Serialize, Deserialize, ParquetRecordWriter, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct Transaction {
    pub block_hash: String,
    pub block_number: String,
    pub from: String,
    pub gas: String,
    pub gas_price: String,
    pub hash: String,
    pub input: String,
    pub nonce: String,
    pub r: String,
    pub s: String,
    pub to: Option<String>,
    pub transaction_index: String,
    #[serde(rename = "type")]
    pub kind: String,
    pub v: String,
    pub value: String,
}
