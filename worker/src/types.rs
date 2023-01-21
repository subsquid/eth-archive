use crate::field_selection::FieldSelection;
use arrayvec::ArrayVec;
use eth_archive_core::deserialize::{Address, Bytes, Bytes32, Index, Sighash};
use eth_archive_core::types::{Log, ResponseBlock, ResponseLog, ResponseTransaction, Transaction};
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub struct MiniQuery {
    pub from_block: u32,
    pub to_block: u32,
    pub logs: Vec<MiniLogSelection>,
    pub transactions: Vec<MiniTransactionSelection>,
    pub field_selection: FieldSelection,
}

#[derive(Clone)]
pub struct MiniLogSelection {
    pub address: Option<Vec<Address>>,
    pub topics: ArrayVec<Vec<Bytes32>, 4>,
}

#[derive(Clone)]
pub struct MiniTransactionSelection {
    pub address: Option<Vec<Address>>,
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

    pub fn matches_tx_dest(&self, dest: &Option<Address>) -> bool {
        self.transactions
            .iter()
            .any(|selection| selection.matches_dest(dest))
    }

    pub fn matches_tx(&self, tx: &Transaction) -> bool {
        self.transactions.iter().any(|selection| {
            selection.matches_dest(&tx.dest)
                && selection.matches_sighash(&tx.input)
                && selection.matches_status(tx.status)
        })
    }
}

impl MiniLogSelection {
    pub fn to_expr(&self) -> Option<Expr> {
        let mut expr = match &self.address {
            Some(addr) if !addr.is_empty() => {
                let address = addr.iter().map(|addr| addr.as_slice()).collect::<Vec<_>>();

                let series = Series::new("", address).lit();
                Some(col("log_address").is_in(series))
            }
            _ => None,
        };

        for (i, topic) in self.topics.iter().enumerate() {
            if !topic.is_empty() {
                let series = topic
                    .iter()
                    .map(|topic| topic.as_slice())
                    .collect::<Vec<_>>();

                let series = Series::new("", series).lit();
                let inner_expr = col(&format!("log_topic{}", i)).is_in(series);

                expr = match expr {
                    Some(expr) => Some(expr.and(inner_expr)),
                    None => Some(inner_expr),
                };
            }
        }

        expr
    }

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
    pub fn to_expr(&self) -> Option<Expr> {
        let mut expr = match &self.address {
            Some(addr) if !addr.is_empty() => {
                let address = addr.iter().map(|addr| addr.as_slice()).collect::<Vec<_>>();
                let series = Series::new("", address).lit();
                Some(col("tx_dest").is_in(series))
            }
            _ => None,
        };

        match &self.sighash {
            Some(sig) if !sig.is_empty() => {
                let sighash = sig.iter().map(|sig| sig.as_slice()).collect::<Vec<_>>();
                let series = Series::new("", sighash).lit();
                let inner_expr = col("tx_sighash").is_in(series);
                expr = match expr {
                    Some(expr) => Some(expr.and(inner_expr)),
                    None => Some(inner_expr),
                };
            }
            _ => (),
        }

        if let Some(status) = self.status {
            let inner_expr = col("tx_status").eq(status.lit());

            expr = match expr {
                Some(expr) => Some(expr.and(inner_expr)),
                None => Some(inner_expr),
            };
        }

        expr
    }

    pub fn matches_dest(&self, dest: &Option<Address>) -> bool {
        if let Some(address) = &self.address {
            let tx_addr = match dest.as_ref() {
                Some(addr) => addr,
                None => return false,
            };

            if !address.is_empty() && !address.iter().any(|addr| addr == tx_addr) {
                return false;
            }
        }

        true
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
    pub block: ResponseBlock,
    pub transactions: BTreeMap<u32, ResponseTransaction>,
    pub logs: BTreeMap<u32, ResponseLog>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BlockEntryVec {
    pub block: ResponseBlock,
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

#[derive(Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Query {
    pub from_block: u32,
    pub to_block: Option<u32>,
    #[serde(default)]
    pub logs: Vec<LogSelection>,
    #[serde(default)]
    pub transactions: Vec<TransactionSelection>,
}

impl Query {
    pub fn field_selection(&self) -> FieldSelection {
        let mut field_selection: FieldSelection = self
            .logs
            .iter()
            .map(|log| log.field_selection)
            .chain(self.transactions.iter().map(|tx| tx.field_selection))
            .fold(Default::default(), |a, b| a | b);

        field_selection.block.number = true;
        field_selection.transaction.hash = true;
        field_selection.transaction.block_number = true;
        field_selection.transaction.transaction_index = true;
        field_selection.transaction.dest = true;
        field_selection.transaction.status = true;
        field_selection.log.block_number = true;
        field_selection.log.log_index = true;
        field_selection.log.transaction_index = true;
        field_selection.log.address = true;
        field_selection.log.topics = true;

        field_selection
    }

    pub fn log_selection(&self) -> Vec<MiniLogSelection> {
        self.logs
            .iter()
            .map(|log| MiniLogSelection {
                address: log.address.clone(),
                topics: log.topics.clone(),
            })
            .collect()
    }

    pub fn tx_selection(&self) -> Vec<MiniTransactionSelection> {
        self.transactions
            .iter()
            .map(|transaction| MiniTransactionSelection {
                address: transaction.address.clone(),
                sighash: transaction.sighash.clone(),
                status: transaction.status,
            })
            .collect()
    }
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct LogSelection {
    pub address: Option<Vec<Address>>,
    pub topics: ArrayVec<Vec<Bytes32>, 4>,
    pub field_selection: FieldSelection,
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TransactionSelection {
    pub address: Option<Vec<Address>>,
    pub sighash: Option<Vec<Sighash>>,
    pub status: Option<u32>,
    pub field_selection: FieldSelection,
}
