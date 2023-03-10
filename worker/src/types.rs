use crate::field_selection::FieldSelection;
use arrayvec::ArrayVec;
use eth_archive_core::deserialize::{Address, Bytes, Bytes32, Index, Sighash};
use eth_archive_core::types::{Log, ResponseBlock, ResponseLog, ResponseTransaction, Transaction};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

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
    pub address: Option<Vec<Address>>,
    pub topics: ArrayVec<Vec<Bytes32>, 4>,
}

#[derive(Clone)]
pub struct MiniTransactionSelection {
    pub source: Option<Vec<Address>>,
    pub dest: Option<Vec<Address>>,
    pub sighash: Option<Vec<Sighash>>,
    pub status: Option<u32>,
}

impl MiniQuery {
    pub fn matches_log_addr(&self, addr: &Address) -> bool {
        self.logs
            .iter()
            .any(|selection| selection.matches_addr(addr))
    }

    pub fn matches_log(&self, log: &Log) -> bool {
        self.logs.iter().any(|selection| {
            selection.matches_addr(&log.address) && selection.matches_topics(&log.topics)
        })
    }

    #[allow(clippy::match_like_matches_macro)]
    pub fn matches_tx(&self, tx: &Transaction) -> bool {
        self.transactions.iter().any(|selection| {
            let source_none = match &selection.source {
                Some(s) if !s.is_empty() => false,
                _ => true,
            };

            let dest_none = match &selection.dest {
                Some(d) if !d.is_empty() => false,
                _ => true,
            };

            let match_all_addr = source_none && dest_none;

            (match_all_addr
                || selection.matches_dest(&tx.dest)
                || selection.matches_source(&tx.source))
                && selection.matches_sighash(&tx.input)
                && selection.matches_status(tx.status)
        })
    }
}

impl MiniLogSelection {
    pub fn matches_addr(&self, filter_addr: &Address) -> bool {
        if let Some(address) = &self.address {
            if !address.is_empty() && !address.iter().any(|addr| addr == filter_addr) {
                return false;
            }
        }

        true
    }

    pub fn matches_topics(&self, topics: &[Bytes32]) -> bool {
        for (topic, log_topic) in self.topics.iter().zip(topics.iter()) {
            if !topic.is_empty() && !topic.iter().any(|topic| log_topic == topic) {
                return false;
            }
        }

        true
    }
}

impl MiniTransactionSelection {
    pub fn matches_dest(&self, dest: &Option<Address>) -> bool {
        if let Some(address) = &self.dest {
            let tx_addr = match dest.as_ref() {
                Some(addr) => addr,
                None => return false,
            };

            if address.iter().any(|addr| addr == tx_addr) {
                return true;
            }
        }

        false
    }

    pub fn matches_source(&self, source: &Option<Address>) -> bool {
        if let Some(address) = &self.source {
            let tx_addr = match source.as_ref() {
                Some(addr) => addr,
                None => return false,
            };

            if address.iter().any(|addr| addr == tx_addr) {
                return true;
            }
        }

        false
    }

    pub fn matches_sighash(&self, input: &Bytes) -> bool {
        if let Some(sighash) = &self.sighash {
            let input = match input.get(..4) {
                Some(sig) => sig,
                None => return false,
            };

            if !sighash.is_empty() && !sighash.iter().any(|sig| sig.as_slice() == input) {
                return false;
            }
        }

        true
    }

    pub fn matches_status(&self, tx_status: Option<Index>) -> bool {
        match (self.status, tx_status) {
            (Some(status), Some(tx_status)) => status == tx_status.0,
            _ => true,
        }
    }
}

pub struct BlockEntry {
    pub block: Option<ResponseBlock>,
    pub transactions: BTreeMap<u32, ResponseTransaction>,
    pub logs: BTreeMap<u32, ResponseLog>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BlockEntryVec {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block: Option<ResponseBlock>,
    pub transactions: Vec<ResponseTransaction>,
    pub logs: Vec<ResponseLog>,
}

impl From<BlockEntry> for BlockEntryVec {
    fn from(entry: BlockEntry) -> Self {
        Self {
            block: entry.block,
            transactions: entry.transactions.into_values().collect(),
            logs: entry.logs.into_values().collect(),
        }
    }
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
    pub address: Option<Vec<Address>>,
    pub topics: ArrayVec<Vec<Bytes32>, 4>,
    pub field_selection: FieldSelection,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct TransactionSelection {
    #[serde(rename = "from")]
    pub source: Option<Vec<Address>>,
    #[serde(rename = "to", alias = "address")]
    pub dest: Option<Vec<Address>>,
    pub sighash: Option<Vec<Sighash>>,
    pub status: Option<u32>,
    pub field_selection: FieldSelection,
}
