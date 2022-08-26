use crate::deserialize::{Address, BigInt, BloomFilterBytes, Bytes, Bytes32, Nonce};
use serde::{Deserialize, Serialize};
use std::cmp;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Block {
    pub number: BigInt,
    pub hash: Bytes32,
    pub parent_hash: Bytes32,
    pub nonce: Nonce,
    pub sha3_uncles: Bytes32,
    pub logs_bloom: BloomFilterBytes,
    pub transactions_root: Bytes32,
    pub state_root: Bytes32,
    pub receipts_root: Bytes32,
    pub miner: Address,
    pub difficulty: Bytes,
    pub total_difficulty: Bytes,
    pub extra_data: Bytes,
    pub size: BigInt,
    pub gas_limit: Bytes,
    pub gas_used: Bytes,
    pub timestamp: BigInt,
    pub transactions: Vec<Transaction>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Transaction {
    pub block_hash: Bytes32,
    pub block_number: BigInt,
    #[serde(alias = "from")]
    pub source: Address,
    pub gas: BigInt,
    pub gas_price: BigInt,
    pub hash: Bytes32,
    pub input: Bytes,
    pub nonce: Nonce,
    #[serde(alias = "to")]
    pub dest: Option<Address>,
    pub transaction_index: BigInt,
    pub value: Bytes,
    #[serde(alias = "type")]
    pub kind: BigInt,
    pub chain_id: BigInt,
    pub v: BigInt,
    pub r: Bytes,
    pub s: Bytes,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Log {
    pub address: Address,
    pub block_hash: Bytes32,
    pub block_number: BigInt,
    pub data: Bytes,
    pub log_index: BigInt,
    pub removed: bool,
    pub topics: Vec<Bytes32>,
    pub transaction_hash: Bytes32,
    pub transaction_index: BigInt,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct BlockRange {
    pub from: usize,
    pub to: usize,
}

impl BlockRange {
    pub fn merge(&self, other: Self) -> Self {
        Self {
            from: cmp::min(self.from, other.from),
            to: cmp::max(self.to, other.to),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ResponseBlock {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub number: Option<BigInt>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hash: Option<Bytes32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_hash: Option<Bytes32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nonce: Option<Nonce>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sha3_uncles: Option<Bytes32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub logs_bloom: Option<BloomFilterBytes>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transactions_root: Option<Bytes32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state_root: Option<Bytes32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub receipts_root: Option<Bytes32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub miner: Option<Address>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub difficulty: Option<Bytes>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_difficulty: Option<Bytes>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extra_data: Option<Bytes>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<BigInt>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gas_limit: Option<Bytes>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gas_used: Option<Bytes>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<BigInt>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ResponseTransaction {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block_hash: Option<Bytes32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block_number: Option<BigInt>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(alias = "from")]
    pub source: Option<Address>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gas: Option<BigInt>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gas_price: Option<BigInt>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hash: Option<Bytes32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input: Option<Bytes>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nonce: Option<Nonce>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(alias = "to")]
    pub dest: Option<Address>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transaction_index: Option<BigInt>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<Bytes>,
    #[serde(alias = "type")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<BigInt>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chain_id: Option<BigInt>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub v: Option<BigInt>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub r: Option<Bytes>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub s: Option<Bytes>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ResponseLog {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub address: Option<Address>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block_hash: Option<Bytes32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block_number: Option<BigInt>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Bytes>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub log_index: Option<BigInt>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub removed: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topics: Option<Vec<Bytes32>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transaction_hash: Option<Bytes32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transaction_index: Option<BigInt>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResponseRow {
    pub block: ResponseBlock,
    pub transaction: ResponseTransaction,
    pub log: ResponseLog,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryMetrics {
    pub build_query: u128,
    pub run_query: u128,
    pub serialize_result: u128,
    pub total: u128,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryResult {
    pub data: Vec<ResponseRow>,
    pub metrics: QueryMetrics,
}
