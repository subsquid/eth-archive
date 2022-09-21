use crate::parquet::BlockData;
use eth_archive_core::deserialize::{Address, BigInt, BloomFilterBytes, Bytes, Bytes32, Nonce};
use eth_archive_core::types::{Block, Log, Transaction};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
struct RawBlock {
    number: u32,
    hash: String,
    parent_hash: String,
    nonce: String,
    sha3_uncles: String,
    logs_bloom: String,
    transactions_root: String,
    state_root: String,
    receipts_root: String,
    miner: String,
    difficulty: u128,
    total_difficulty: u128,
    extra_data: String,
    size: u32,
    gas_limit: u32,
    gas_used: u32,
    timestamp: u32,
}

#[derive(Serialize, Deserialize, Debug)]
struct RawTransaction {
    block_hash: String,
    block_number: u32,
    source: String,
    gas: u128,
    gas_price: u128,
    hash: String,
    input: String,
    nonce: String,
    dest: Option<String>,
    transaction_index: u32,
    value: String,
    kind: u32,
    chain_id: u32,
    v: u32,
    r: String,
    s: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct RawLog {
    address: String,
    block_hash: String,
    block_number: u32,
    data: String,
    log_index: u32,
    removed: bool,
    topics: Vec<String>,
    transaction_hash: String,
    transaction_index: u32,
}

#[derive(Serialize, Deserialize, Debug)]
struct RawData {
    block: RawBlock,
    transactions: Vec<RawTransaction>,
    logs: Vec<RawLog>,
}

fn decode_hex(value: &str) -> Vec<u8> {
    prefix_hex::decode(value).unwrap()
}

impl From<RawBlock> for Block {
    fn from(raw: RawBlock) -> Block {
        Block {
            number: BigInt(raw.number.into()),
            hash: Bytes32::new(&decode_hex(&raw.hash)),
            parent_hash: Bytes32::new(&decode_hex(&raw.parent_hash)),
            nonce: Nonce::new(&decode_hex(&raw.nonce)),
            sha3_uncles: Bytes32::new(&decode_hex(&raw.sha3_uncles)),
            logs_bloom: BloomFilterBytes::new(&decode_hex(&raw.logs_bloom)),
            transactions_root: Bytes32::new(&decode_hex(&raw.transactions_root)),
            state_root: Bytes32::new(&decode_hex(&raw.state_root)),
            receipts_root: Bytes32::new(&decode_hex(&raw.receipts_root)),
            miner: Address::new(&decode_hex(&raw.miner)),
            difficulty: Bytes::new(raw.difficulty.to_string().as_bytes()),
            total_difficulty: Bytes::new(raw.total_difficulty.to_string().as_bytes()),
            extra_data: Bytes::new(&decode_hex(&raw.extra_data)),
            size: BigInt(raw.size.into()),
            gas_limit: Bytes::new(raw.gas_limit.to_string().as_bytes()),
            gas_used: Bytes::new(raw.gas_used.to_string().as_bytes()),
            timestamp: BigInt(raw.timestamp.into()),
            transactions: vec![],
        }
    }
}

impl From<RawTransaction> for Transaction {
    fn from(raw: RawTransaction) -> Transaction {
        Transaction {
            block_hash: Bytes32::new(&decode_hex(&raw.block_hash)),
            block_number: BigInt(raw.block_number.into()),
            source: Address::new(&decode_hex(&raw.source)),
            gas: BigInt(raw.gas.try_into().unwrap()),
            gas_price: BigInt(raw.gas_price.try_into().unwrap()),
            hash: Bytes32::new(&decode_hex(&raw.hash)),
            input: Bytes::new(&decode_hex(&raw.input)),
            nonce: Nonce::new(&decode_hex(&raw.nonce)),
            dest: raw.dest.map(|dest| Address::new(&decode_hex(&dest))),
            transaction_index: BigInt(raw.transaction_index.into()),
            value: Bytes::new(raw.value.as_bytes()),
            kind: BigInt(raw.kind.into()),
            chain_id: BigInt(raw.chain_id.into()),
            v: BigInt(raw.v.into()),
            r: Bytes::new(raw.r.as_bytes()),
            s: Bytes::new(raw.s.as_bytes()),
        }
    }
}

impl From<RawLog> for Log {
    fn from(raw: RawLog) -> Log {
        Log {
            address: Address::new(&decode_hex(&raw.address)),
            block_hash: Bytes32::new(&decode_hex(&raw.block_hash)),
            block_number: BigInt(raw.block_number.into()),
            data: Bytes::new(&decode_hex(&raw.data)),
            log_index: BigInt(raw.log_index.into()),
            removed: raw.removed,
            topics: raw
                .topics
                .into_iter()
                .map(|topic| Bytes32::new(&decode_hex(&topic)))
                .collect(),
            transaction_hash: Bytes32::new(&decode_hex(&raw.transaction_hash)),
            transaction_index: BigInt(raw.transaction_index.into()),
        }
    }
}

impl From<RawData> for BlockData {
    fn from(raw: RawData) -> BlockData {
        BlockData {
            block: Block::from(raw.block),
            transactions: raw
                .transactions
                .into_iter()
                .map(Transaction::from)
                .collect(),
            logs: raw.logs.into_iter().map(Log::from).collect(),
        }
    }
}

fn read_raw_data_from_file(path: &str) -> Vec<RawData> {
    let content = std::fs::read_to_string(path).unwrap();
    serde_json::from_str(&content).unwrap()
}

fn convert_raw_data(block_data: Vec<RawData>) -> Vec<BlockData> {
    block_data.into_iter().map(BlockData::from).collect()
}

pub fn read_data_from_file(path: &str) -> Vec<BlockData> {
    let raw_data = read_raw_data_from_file(path);
    convert_raw_data(raw_data)
}
