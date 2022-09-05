use crate::field_selection::FieldSelection;
use crate::{Error, Result};
use datafusion::prelude::*;
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
    pub address: String,
    pub topics: Vec<Vec<String>>,
}

impl AddressQuery {
    pub fn to_expr(&self) -> Result<Expr> {
        let address =
            prefix_hex::decode::<Vec<u8>>(&self.address).map_err(Error::InvalidHexInAddress)?;

        let mut expr = col("log.address").eq(lit(address));

        if self.topics.len() > 4 {
            return Err(Error::TooManyTopics(self.topics.len()));
        }

        for (i, topic) in self.topics.iter().enumerate() {
            if !topic.is_empty() {
                let topic = topic
                    .iter()
                    .map(|topic| {
                        Ok(lit(prefix_hex::decode::<Vec<u8>>(topic)
                            .map_err(Error::InvalidHexInTopic)?))
                    })
                    .collect::<Result<_>>()?;
                expr = expr.and(col(&format!("log.topic{}", i)).in_list(topic, false));
            }
        }

        Ok(expr)
    }

    pub fn to_sql(&self) -> Result<String> {
        let mut sql = format!(
            "(
            eth_log.address = decode('{}', 'hex')",
            self.address
                .strip_prefix("0x")
                .ok_or(Error::InvalidAddress)?
        );

        if self.topics.len() > 4 {
            return Err(Error::TooManyTopics(self.topics.len()));
        }

        for (i, topic) in self.topics.iter().enumerate() {
            if !topic.is_empty() {
                let topics = topic
                    .iter()
                    .map(|topic| {
                        topic
                            .strip_prefix("0x")
                            .map(|topic| format!("decode('{}', 'hex')", topic))
                            .ok_or(Error::InvalidTopic)
                    })
                    .collect::<Result<Vec<String>>>()?
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
