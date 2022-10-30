use crate::field_selection::FieldSelection;
use crate::{Error, Result};
use eth_archive_core::deserialize::{Address, Bytes32, Sighash};
use eth_archive_core::types::{ResponseBlock, ResponseLog, ResponseTransaction};
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use std::fmt::Write;

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
    pub topics: Vec<Vec<Bytes32>>,
}

#[derive(Deserialize, Clone)]
pub struct MiniTransactionSelection {
    pub address: Option<Vec<Address>>,
    pub sighash: Option<Sighash>,
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

        if self.topics.len() > 4 {
            return Err(Error::TooManyTopics(self.topics.len()));
        }

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
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Status {
    pub parquet_block_number: u32,
    pub db_max_block_number: u32,
    pub db_min_block_number: u32,
    pub archive_height: u32,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BlockEntry {
    pub block: ResponseBlock,
    pub transactions: Vec<ResponseTransaction>,
    pub logs: Vec<ResponseLog>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Query {
    pub from_block: u32,
    pub to_block: Option<u32>,
    pub logs: Vec<LogSelection>,
    pub transactions: Vec<TransactionSelection>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LogSelection {
    pub address: Option<Vec<Address>>,
    pub topics: Vec<Vec<Bytes32>>,
    pub field_selection: Option<FieldSelection>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransactionSelection {
    pub address: Option<Vec<Address>>,
    pub sighash: Option<Sighash>,
    pub field_selection: Option<FieldSelection>,
}
