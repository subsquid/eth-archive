use crate::field_selection::FieldSelection;
use crate::{Error, Result};
use datafusion::prelude::*;
use eth_archive_core::types::ResponseRow;
use serde::{Deserialize, Serialize};

use serde_json::Value as JsonValue;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryLogs {
    pub from_block: u64,
    pub to_block: u64,
    pub addresses: Vec<AddressQuery>,
    pub field_selection: FieldSelection,
}

impl QueryLogs {
    pub fn to_sql(&self) -> String {
        /*
        format!(
            "
            SELECT {} FROM log
            JOIN block ON block.number = log.block_number
            JOIN transaction ON
                transaction.block_number = log.block_number AND
                    transaction.transaction_index = log.transaction_index
            WHERE log.block_number < {} AND log.block_number >= {} AND
                log.address IN {} AND log.topics
        "
        )
        */
        todo!()
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AddressQuery {
    pub address: String,
    pub topics: [Option<Vec<String>>; 4],
}

impl AddressQuery {
    pub fn to_expr(&self) -> Result<Expr> {
        let address =
            prefix_hex::decode::<Vec<u8>>(&self.address).map_err(Error::InvalidHexInAddress)?;

        let mut expr = col("log.address").eq(lit(address));

        for (i, topic) in self.topics.iter().enumerate() {
            if let Some(topic) = topic {
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
        }

        Ok(expr)
    }
}

#[derive(Serialize, Deserialize)]
pub struct Status {
    pub parquet_block_number: u64,
    pub db_max_block_number: usize,
    pub db_min_block_number: usize,
}

#[derive(Serialize, Deserialize)]
pub struct QueryResult {
    pub data: Vec<ResponseRow>,
}
