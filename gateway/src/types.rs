use crate::field_selection::FieldSelection;
use crate::{Error, Result};
use datafusion::prelude::*;
use eth_archive_core::deserialize::{Address, Bytes32};
use eth_archive_core::types::{QueryMetrics, ResponseRow};
use serde::{Deserialize, Serialize};
use std::fmt::Write;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryLogs {
    pub from_block: u32,
    pub to_block: u32,
    pub addresses: Vec<AddressQuery>,
    pub field_selection: FieldSelection,
}

impl QueryLogs {
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

        if !self.addresses.is_empty() {
            query += "AND (";

            query += &self.addresses.get(0).unwrap().to_sql()?;

            for addr in self.addresses.iter().skip(1) {
                query += " OR ";
                query += &addr.to_sql()?;
            }

            query.push(')');
        }

        Ok(query)
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AddressQuery {
    pub address: Address,
    pub topics: Vec<Vec<Bytes32>>,
    pub sighashes: Vec<Bytes32>,
}

impl AddressQuery {
    pub fn to_expr(&self) -> Result<Expr> {
        let mut expr = col("log.address").eq(lit(self.address.0.to_vec()));

        if self.topics.len() > 4 {
            return Err(Error::TooManyTopics(self.topics.len()));
        }

        for (i, topic) in self.topics.iter().enumerate() {
            if !topic.is_empty() {
                let topic = topic.iter().map(|topic| lit(topic.to_vec())).collect();
                expr = expr.and(col(&format!("log.topic{}", i)).in_list(topic, false));
            }
        }

        if !self.sighashes.is_empty() {
            let mut sighashes = self.sighashes.iter();

            let sighash_to_expr =
                |sighash: &Bytes32| starts_with(col("tx.input"), lit(sighash.to_vec()));

            let mut filter_expr = sighash_to_expr(sighashes.next().unwrap());

            for sighash in sighashes {
                filter_expr = filter_expr.or(sighash_to_expr(sighash));
            }

            expr = expr.and(filter_expr);
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

        if !self.sighashes.is_empty() {
            let sighashes = self
                .sighashes
                .iter()
                .map(|sighash| {
                    let sighash = prefix_hex::encode(&*sighash.0);
                    let sighash = sighash.strip_prefix("0x").unwrap();
                    format!("encode(eth_tx.input, 'hex') ILIKE '{}%'", sighash)
                })
                .collect::<Vec<String>>()
                .join(" OR ");

            write!(&mut sql, " AND ({})", sighashes).unwrap();
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
pub struct QueryResponse {
    pub status: Status,
    pub data: Vec<ResponseRow>,
    pub metrics: QueryMetrics,
}
