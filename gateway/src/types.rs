use crate::field_selection::FieldSelection;
use crate::{Error, Result};
use eth_archive_core::deserialize::{Address, Bytes32};
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
    pub field_selection: FieldSelection,
}

#[derive(Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MiniLogSelection {
    pub address: Address,
    pub topics: Vec<Vec<Bytes32>>,
}

impl MiniQuery {
    pub fn to_sql(&self) -> Result<String> {
        let mut query = format!(
            "
            SELECT {} FROM eth_log
            JOIN eth_block ON eth_block.number = eth_log.block_number
            JOIN eth_tx ON
                eth_tx.block_number = eth_log.block_number AND
                    eth_tx.transaction_index = eth_log.transaction_index
            WHERE eth_log.block_number < {} AND eth_log.block_number >= {}
        ",
            self.field_selection.to_cols_sql(),
            self.to_block,
            self.from_block,
        );

        if !self.logs.is_empty() {
            query += "AND (";

            query += &self.logs.get(0).unwrap().to_sql()?;

            for log in self.logs.iter().skip(1) {
                query += " OR ";
                query += &log.to_sql()?;
            }

            query.push(')');
        }

        Ok(query)
    }
}

impl MiniLogSelection {
    pub fn to_expr(&self) -> Result<Expr> {
        let mut expr = col("log_address").eq(lit(self.address.to_vec()));

        if self.topics.len() > 4 {
            return Err(Error::TooManyTopics(self.topics.len()));
        }

        for (i, topic) in self.topics.iter().enumerate() {
            if !topic.is_empty() {
                let series = topic
                    .iter()
                    .map(|topic| topic.as_slice())
                    .collect::<Vec<_>>();
                let series = Series::new("series", series).lit();
                expr = expr.and(col(&format!("log_topic{}", i)).is_in(series));
            }
        }

        Ok(expr)
    }

    pub fn to_sql(&self) -> Result<String> {
        let mut sql = format!(
            "(
            eth_log.address = decode('{}', 'hex')",
            prefix_hex::encode(&*self.address.0)
                .strip_prefix("0x")
                .unwrap()
        );

        if self.topics.len() > 4 {
            return Err(Error::TooManyTopics(self.topics.len()));
        }

        for (i, topic) in self.topics.iter().enumerate() {
            if !topic.is_empty() {
                let topics = topic
                    .iter()
                    .map(|topic| {
                        let topic = prefix_hex::encode(&*topic.0);
                        let topic = topic.strip_prefix("0x").unwrap();
                        format!("decode('{}', 'hex')", topic)
                    })
                    .collect::<Vec<String>>()
                    .join(", ");
                write!(&mut sql, " AND eth_log.topic{} IN ({})", i, topics).unwrap();
            }
        }

        sql.push(')');

        Ok(sql)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Status {
    pub parquet_block_number: u32,
    pub db_max_block_number: usize,
    pub db_min_block_number: usize,
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
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LogSelection {
    pub address: Address,
    pub topics: Vec<Vec<Bytes32>>,
    pub field_selection: Option<FieldSelection>,
}
