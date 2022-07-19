use serde::{Deserialize, Serialize};
use crate::deserialize::{BigInt, Bytes32, Address, BloomFilterBytes, Nonce};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AnanBlock {
    pub number: String,
    pub hash: String,
    pub parent_hash: String,
    pub nonce: String,
    pub sha3_uncles: String,
    pub logs_bloom: String,
    pub transactions_root: String,
    pub state_root: String,
    pub receipts_root: String,
    pub miner: String,
    pub difficulty: String,
    pub total_difficulty: String,
    pub extra_data: String,
    pub size: String,
    pub gas_limit: String,
    pub gas_used: String,
    pub timestamp: String,
    pub transactions: Vec<Transaction>,
}

#[derive(Debug, Clone, Deserialize)]
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
    pub difficulty: BigInt,
    pub total_difficulty: BigInt,
    pub extra_data: Vec<u8>,
    pub size: BigInt,
    pub gas_limit: BigInt,
    pub gas_used: BigInt,
    pub timestamp: BigInt,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Transaction {
    pub block_hash: Option<String>,
    pub block_number: Option<String>,
    pub from: String,
    pub gas: String,
    pub gas_price: String,
    pub hash: String,
    pub input: String,
    pub nonce: String,
    pub to: Option<String>,
    pub transaction_index: Option<String>,
    pub value: String,
    pub v: String,
    pub r: String,
    pub s: String,
}

#[derive(Debug, Clone)]
pub struct DbTransaction {
    pub block_hash: Bytes32,
    pub block_number: i64,
    pub from: Address,
    pub gas: i64,
    pub gas_price: i64,
    pub hash: Bytes32,
    pub input: Vec<u8>,
    pub nonce: Nonce,
    pub to: Option<Address>,
    pub transaction_index: Option<i64>,
    pub value: Bytes32,
    pub v: i64,
    pub r: Bytes32,
    pub s: Bytes32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Log {
    pub address: String,
    pub block_hash: String,
    pub block_number: String,
    pub data: String,
    pub log_index: String,
    pub removed: bool,
    pub topics: Vec<String>,
    pub transaction_hash: String,
    pub transaction_index: Option<String>,
}

#[derive(Debug, Clone)]
pub struct DbLog {
    pub address: Address,
    pub block_hash: Bytes32,
    pub block_number: i64,
    pub data: Vec<u8>,
    pub log_index: i64,
    pub removed: bool,
    pub topic0: Option<Bytes32>,
    pub topic1: Option<Bytes32>,
    pub topic2: Option<Bytes32>,
    pub topic3: Option<Bytes32>,
    pub transaction_hash: Bytes32,
    pub transaction_index: i64,
}
