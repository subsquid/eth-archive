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

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResponseBlock {
    pub number: Option<BigInt>,
    pub hash: Option<Bytes32>,
    pub parent_hash: Option<Bytes32>,
    pub nonce: Option<Nonce>,
    pub sha3_uncles: Option<Bytes32>,
    pub logs_bloom: Option<BloomFilterBytes>,
    pub transactions_root: Option<Bytes32>,
    pub state_root: Option<Bytes32>,
    pub receipts_root: Option<Bytes32>,
    pub miner: Option<Address>,
    pub difficulty: Option<Bytes>,
    pub total_difficulty: Option<Bytes>,
    pub extra_data: Option<Bytes>,
    pub size: Option<BigInt>,
    pub gas_limit: Option<Bytes>,
    pub gas_used: Option<Bytes>,
    pub timestamp: Option<BigInt>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResponseTransaction {
    pub block_hash: Option<Bytes32>,
    pub block_number: Option<BigInt>,
    #[serde(alias = "from")]
    pub source: Option<Address>,
    pub gas: Option<BigInt>,
    pub gas_price: Option<BigInt>,
    pub hash: Option<Bytes32>,
    pub input: Option<Bytes>,
    pub nonce: Option<Nonce>,
    #[serde(alias = "to")]
    pub dest: Option<Address>,
    pub transaction_index: Option<BigInt>,
    pub value: Option<Bytes>,
    #[serde(alias = "type")]
    pub kind: Option<BigInt>,
    pub chain_id: Option<BigInt>,
    pub v: Option<BigInt>,
    pub r: Option<Bytes>,
    pub s: Option<Bytes>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResponseLog {
    pub address: Option<Address>,
    pub block_hash: Option<Bytes32>,
    pub block_number: Option<BigInt>,
    pub data: Option<Bytes>,
    pub log_index: Option<BigInt>,
    pub removed: Option<bool>,
    pub topics: Option<Vec<Bytes32>>,
    pub transaction_hash: Option<Bytes32>,
    pub transaction_index: Option<BigInt>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResponseRow {
    block: ResponseBlock,
    transaction: ResponseTransaction,
    log: ResponseLog,
}
