use crate::field_selection::FieldSelection;
use crate::{Error, Result};
use arrayvec::ArrayVec;
use eth_archive_core::deserialize::{Address, Bytes32, Sighash};
use eth_archive_core::types::{Log, ResponseBlock, ResponseLog, ResponseTransaction, Transaction};
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MiniQuery {
    pub from_block: u32,
    pub to_block: u32,
    pub logs: Vec<MiniLogSelection>,
    pub transactions: Vec<MiniTransactionSelection>,
    pub field_selection: FieldSelection,
}

#[derive(Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MiniLogSelection {
    pub address: Option<Vec<Address>>,
    pub topics: ArrayVec<Vec<Bytes32>, 4>,
}

#[derive(Deserialize, Clone)]
pub struct MiniTransactionSelection {
    pub address: Option<Vec<Address>>,
    pub sighash: Option<Sighash>,
}

impl MiniQuery {
    pub fn matches_log(&self, log: &Log) -> bool {
        let block_num = log.block_number.0;

        if block_num < self.from_block || block_num >= self.to_block {
            return false;
        }

        self.logs.iter().any(|selection| selection.matches(log))
    }

    pub fn matches_tx(&self, tx: &Transaction) -> bool {
        let block_num = tx.block_number.0;

        if block_num < self.from_block || block_num >= self.to_block {
            return false;
        }

        self.transactions
            .iter()
            .any(|selection| selection.matches(tx))
    }
}

impl MiniLogSelection {
    pub fn to_expr(&self) -> Result<Option<Expr>> {
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

        Ok(expr)
    }

    pub fn matches(&self, log: &Log) -> bool {
        if let Some(address) = &self.address {
            if !address.iter().any(|addr| addr == &log.address) {
                return false;
            }
        }

        for (topic, log_topic) in self.topics.iter().zip(log.topics.iter()) {
            if !topic.is_empty() && !topic.iter().any(|topic| log_topic == topic) {
                return false;
            }
        }

        true
    }
}

impl MiniTransactionSelection {
    pub fn to_expr(&self) -> Result<Option<Expr>> {
        let mut expr = match &self.address {
            Some(addr) if !addr.is_empty() => {
                let address = addr.iter().map(|addr| addr.as_slice()).collect::<Vec<_>>();

                let series = Series::new("", address).lit();
                Some(col("tx_dest").is_in(series))
            }
            _ => None,
        };

        if let Some(sighash) = &self.sighash {
            let inner_expr = col("tx_sighash").eq(lit(sighash.0.as_slice()));
            expr = match expr {
                Some(expr) => Some(expr.and(inner_expr)),
                None => Some(inner_expr),
            };
        }

        Ok(expr)
    }

    pub fn matches(&self, tx: &Transaction) -> bool {
        if let Some(address) = &self.address {
            let tx_addr = match tx.dest.as_ref() {
                Some(addr) => addr,
                None => return false,
            };

            if !address.iter().any(|addr| addr == tx_addr) {
                return false;
            }
        }

        if let Some(sighash) = &self.sighash {
            match tx.input.get(..4) {
                Some(sig) => {
                    if sig != sighash.as_slice() {
                        return false;
                    }
                }
                None => return false,
            }
        }

        true
    }
}

pub struct BlockEntry {
    pub block: ResponseBlock,
    pub transactions: BTreeMap<u32, ResponseTransaction>,
    pub logs: BTreeMap<u32, ResponseLog>,
}

#[derive(Serialize)]
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

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Query {
    pub from_block: u32,
    pub to_block: Option<u32>,
    pub logs: Vec<LogSelection>,
    pub transactions: Vec<TransactionSelection>,
}

impl Query {
    pub fn field_selection(&self) -> Result<FieldSelection> {
        let mut field_selection = None;
        for log in &self.logs {
            field_selection = FieldSelection::merge(field_selection, log.field_selection);
        }
        for tx in &self.transactions {
            field_selection = FieldSelection::merge(field_selection, tx.field_selection);
        }
        let mut field_selection = field_selection.ok_or(Error::NoFieldsSelected)?;

        let mut block_selection = field_selection.block.unwrap_or_default();
        let mut tx_selection = field_selection.transaction.unwrap_or_default();
        let mut log_selection = field_selection.log.unwrap_or_default();

        block_selection.number = Some(true);
        tx_selection.hash = Some(true);
        tx_selection.block_number = Some(true);
        tx_selection.transaction_index = Some(true);
        tx_selection.dest = Some(true);
        log_selection.block_number = Some(true);
        log_selection.log_index = Some(true);
        log_selection.transaction_index = Some(true);
        log_selection.address = Some(true);
        log_selection.topics = Some(true);

        field_selection.block = Some(block_selection);
        field_selection.transaction = Some(tx_selection);
        field_selection.log = Some(log_selection);

        Ok(field_selection)
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
            })
            .collect()
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LogSelection {
    pub address: Option<Vec<Address>>,
    pub topics: ArrayVec<Vec<Bytes32>, 4>,
    pub field_selection: Option<FieldSelection>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransactionSelection {
    pub address: Option<Vec<Address>>,
    pub sighash: Option<Sighash>,
    pub field_selection: Option<FieldSelection>,
}
