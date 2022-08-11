use crate::field_selection::FieldSelection;
use datafusion::prelude::*;
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
        format!(
            "
            SELECT {} FROM log
            JOIN block ON block.number = log.block_number
            JOIN transaction ON
                transaction.block_number = log.block_number AND
                    transaction.transaction_index = log.transaction_index
            WHERE log.block_number < {} AND log.block_number >= {} AND
                log.address IN {}
        "
        )
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AddressQuery {
    pub address: String,
    pub topics: [Option<Vec<String>>; 4],
}

impl From<AddressQuery> for Expr {
    fn from(query: AddressQuery) -> Expr {
        let mut expr = col("log.address").eq(lit(query.address));

        for (i, topic) in query.topics.into_iter().enumerate() {
            if let Some(topic) = topic {
                if !topic.is_empty() {
                    let topic = topic.into_iter().map(lit).collect();
                    expr = expr.and(col(&format!("log.topic{}", i)).in_list(topic, false));
                }
            }
        }

        expr
    }
}

#[derive(Serialize, Deserialize)]
pub struct Status {
    pub parquet_block_number: u64,
    pub db_block_number: usize,
}

#[derive(Serialize, Deserialize)]
pub struct QueryResult {
    pub data: Vec<serde_json::Map<String, JsonValue>>,
}
