use crate::deserialize::{Address, BigInt, BloomFilterBytes, Bytes, Bytes32, Nonce};
use serde::Deserialize;

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
    pub extra_data: Bytes,
    pub size: BigInt,
    pub gas_limit: BigInt,
    pub gas_used: BigInt,
    pub timestamp: BigInt,
    pub transactions: Vec<Transaction>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Transaction {
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

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Log {
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
