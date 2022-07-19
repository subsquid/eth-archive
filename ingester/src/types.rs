use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Block {
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

#[derive(Debug, Clone)]
pub struct DbBlock {
    pub number: i64,
    pub hash: Box<[u8; 32]>,
    pub parent_hash: Box<[u8; 32]>,
    pub nonce: Box<[u8; 16]>,
    pub sha3_uncles: Box<[u8; 32]>,
    pub logs_bloom: Box<[u8; 256]>,
    pub transactions_root: Box<[u8; 32]>,
    pub state_root: Box<[u8; 32]>,
    pub receipts_root: Box<[u8; 32]>,
    pub miner: Box<[u8; 20]>,
    pub difficulty: i64,
    pub total_difficulty: Box<[u8; 32]>,
    pub extra_data: Vec<u8>,
    pub size: i64,
    pub gas_limit: i64,
    pub gas_used: i64,
    pub timestamp: i64,
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
    pub block_hash: Box<[u8; 32]>,
    pub block_number: i64,
    pub from: Box<[u8; 20]>,
    pub gas:i64,
    pub gas_price: i64,
    pub hash: Box<[u8; 32]>,
    pub input: Vec<u8>,
    pub nonce: Box<[u8; 16]>,
    pub to: Option<Box<[u8; 20]>>,
    pub transaction_index: Option<i64>,
    pub value: Box<[u8; 32]>,
    pub v: i64,
    pub r: Box<[u8; 32]>,
    pub s: Box<[u8; 32]>,
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
    pub address: Box<[u8; 20]>,
    pub block_hash: Box<[u8; 32]>,
    pub block_number: i64,
    pub data: Vec<u8>,
    pub log_index: i64,
    pub removed: bool,
    pub topic0: Option<Box<[u8; 32]>>,
    pub topic1: Option<Box<[u8; 32]>>,
    pub topic2: Option<Box<[u8; 32]>>,
    pub topic3: Option<Box<[u8; 32]>>,
    pub transaction_hash: Box<[u8; 32]>,
    pub transaction_index: i64,
}
