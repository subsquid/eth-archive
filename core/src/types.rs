use crate::deserialize::{Address, BigUnsigned, BloomFilterBytes, Bytes, Bytes32, Index};
use crate::{Error, Result};
use arrayvec::ArrayVec;
use core::str::FromStr;
use serde::{Deserialize, Serialize};
use std::cmp;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Block {
    pub parent_hash: Bytes32,
    pub sha3_uncles: Bytes32,
    pub miner: Address,
    pub state_root: Bytes32,
    pub transactions_root: Bytes32,
    pub receipts_root: Bytes32,
    pub logs_bloom: BloomFilterBytes,
    pub difficulty: Option<Bytes>,
    pub number: Index,
    pub gas_limit: Bytes,
    pub gas_used: Bytes,
    pub timestamp: Bytes,
    pub extra_data: Bytes,
    pub mix_hash: Option<Bytes32>,
    pub nonce: Option<BigUnsigned>,
    pub total_difficulty: Option<Bytes>,
    pub base_fee_per_gas: Option<Bytes>,
    pub size: Bytes,
    pub hash: Option<Bytes32>,
    pub transactions: Vec<Transaction>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Transaction {
    #[serde(rename = "type")]
    pub kind: Option<Index>,
    pub nonce: BigUnsigned,
    #[serde(rename = "to")]
    pub dest: Option<Address>,
    pub gas: Bytes,
    pub value: Bytes,
    pub input: Bytes,
    pub max_priority_fee_per_gas: Option<Bytes>,
    pub max_fee_per_gas: Option<Bytes>,
    pub y_parity: Option<Index>,
    pub chain_id: Option<Index>,
    pub v: Option<BigUnsigned>,
    pub r: Option<Bytes>,
    pub s: Option<Bytes>,
    #[serde(rename = "from")]
    pub source: Option<Address>,
    pub block_hash: Bytes32,
    pub block_number: Index,
    pub transaction_index: Index,
    pub gas_price: Option<Bytes>,
    pub hash: Bytes32,
    pub status: Option<Index>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TransactionReceipt {
    pub block_number: Index,
    pub transaction_index: Index,
    pub logs: Option<Vec<Log>>,
    pub status: Option<Index>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Log {
    pub address: Address,
    pub block_hash: Bytes32,
    pub block_number: Index,
    pub data: Bytes,
    pub log_index: Index,
    pub removed: Option<bool>,
    pub topics: ArrayVec<Bytes32, 4>,
    pub transaction_hash: Bytes32,
    pub transaction_index: Index,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ResponseBlock {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_hash: Option<Bytes32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sha3_uncles: Option<Bytes32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub miner: Option<Address>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state_root: Option<Bytes32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transactions_root: Option<Bytes32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub receipts_root: Option<Bytes32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub logs_bloom: Option<BloomFilterBytes>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub difficulty: Option<Bytes>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub number: Option<Index>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gas_limit: Option<Bytes>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gas_used: Option<Bytes>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<Bytes>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extra_data: Option<Bytes>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mix_hash: Option<Bytes32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nonce: Option<BigUnsigned>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_difficulty: Option<Bytes>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_fee_per_gas: Option<Bytes>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<Bytes>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hash: Option<Bytes32>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ResponseTransaction {
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "type")]
    pub kind: Option<Index>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nonce: Option<BigUnsigned>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "to")]
    pub dest: Option<Address>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gas: Option<Bytes>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<Bytes>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input: Option<Bytes>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_priority_fee_per_gas: Option<Bytes>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_fee_per_gas: Option<Bytes>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub y_parity: Option<Index>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chain_id: Option<Index>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub v: Option<BigUnsigned>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub r: Option<Bytes>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub s: Option<Bytes>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "from")]
    pub source: Option<Address>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block_hash: Option<Bytes32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block_number: Option<Index>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "index")]
    pub transaction_index: Option<Index>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gas_price: Option<Bytes>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hash: Option<Bytes32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<Index>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ResponseLog {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub address: Option<Address>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block_hash: Option<Bytes32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block_number: Option<Index>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Bytes>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "index")]
    pub log_index: Option<Index>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub removed: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topics: Option<ArrayVec<Bytes32, 4>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transaction_hash: Option<Bytes32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transaction_index: Option<Index>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct BlockRange {
    pub from: u32,
    pub to: u32,
}

impl std::ops::Add for BlockRange {
    type Output = Self;

    fn add(self, other: Self) -> Self::Output {
        Self {
            from: cmp::min(self.from, other.from),
            to: cmp::max(self.to, other.to),
        }
    }
}

impl std::ops::AddAssign for BlockRange {
    fn add_assign(&mut self, other: Self) {
        self.from = cmp::min(self.from, other.from);
        self.to = cmp::max(self.to, other.to);
    }
}

#[derive(Clone, Copy, Debug)]
pub enum FormatVersion {
    Ver0_0_39,
    Ver0_1_0,
}

impl FromStr for FormatVersion {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "0.0.39" => Ok(FormatVersion::Ver0_0_39),
            "0.1.0" => Ok(FormatVersion::Ver0_1_0),
            _ => Err(Error::UnknownFormat(s.to_owned())),
        }
    }
}
