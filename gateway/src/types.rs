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
    pub sighash: Option<Bytes32>,
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

        if let Some(ref addr) = self.sighash {
            write!(
                &mut query,
                "AND eth_tx.input LIKE '{}%'",
                prefix_hex::encode(&*addr.0).strip_prefix("0x").unwrap()
            )
            .unwrap();
        }

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
                        prefix_hex::encode(&*topic.0)
                            .strip_prefix("0x")
                            .unwrap()
                            .to_owned()
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
pub struct QueryResponse {
    pub status: Status,
    pub data: Vec<ResponseRow>,
    pub metrics: QueryMetrics,
}
