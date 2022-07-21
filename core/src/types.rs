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
    pub difficulty: Bytes,
    pub total_difficulty: Bytes,
    pub extra_data: Bytes,
    pub size: BigInt,
    pub gas_limit: Bytes,
    pub gas_used: Bytes,
    pub timestamp: BigInt,
    pub transactions: Vec<Transaction>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Transaction {
    pub block_hash: Bytes32,
    pub block_number: BigInt,
    pub from: Address,
    pub gas: BigInt,
    pub gas_price: BigInt,
    pub hash: Bytes32,
    pub input: Bytes,
    pub nonce: Nonce,
    pub to: Option<Address>,
    pub transaction_index: BigInt,
    pub value: Bytes,
    pub v: BigInt,
    pub r: Bytes,
    pub s: Bytes,
}

#[derive(Debug, Clone, Deserialize)]
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
