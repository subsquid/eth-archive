use crate::field_selection::FieldSelection;
use arrayvec::ArrayVec;
use eth_archive_core::deserialize::{Address, Bytes32, Sighash};
use serde::{Deserialize, Serialize};

pub struct MiniQuery {
    pub from_block: u32,
    pub to_block: u32,
    pub logs: Vec<MiniLogSelection>,
    pub transactions: Vec<MiniTransactionSelection>,
    pub field_selection: FieldSelection,
    pub include_all_blocks: bool,
}

#[derive(Clone)]
pub struct MiniLogSelection {
    pub address: Vec<Address>,
    pub topics: ArrayVec<Vec<Bytes32>, 4>,
}

#[derive(Clone)]
pub struct MiniTransactionSelection {
    pub source: Vec<Address>,
    pub dest: Vec<Address>,
    pub sighash: Vec<Sighash>,
    pub status: Option<u32>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Query {
    pub from_block: u32,
    pub to_block: Option<u32>,
    #[serde(default)]
    pub logs: Vec<LogSelection>,
    #[serde(default)]
    pub transactions: Vec<TransactionSelection>,
    #[serde(default)]
    pub include_all_blocks: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct LogSelection {
    #[serde(default)]
    pub address: Vec<Address>,
    pub topics: ArrayVec<Vec<Bytes32>, 4>,
    pub field_selection: FieldSelection,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct TransactionSelection {
    #[serde(rename = "from")]
    #[serde(default)]
    pub source: Vec<Address>,
    #[serde(rename = "to", alias = "address")]
    #[serde(default)]
    pub dest: Vec<Address>,
    #[serde(default)]
    pub sighash: Vec<Sighash>,
    pub status: Option<u32>,
    pub field_selection: FieldSelection,
}
