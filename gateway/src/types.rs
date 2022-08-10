use crate::field_selection::LogFieldSelection;
use datafusion::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryLogs {
    pub from_block: u64,
    pub to_block: u64,
    pub addresses: Vec<AddressQuery>,
    pub field_selection: LogFieldSelection,
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
pub struct Status {}

#[derive(Serialize, Deserialize)]
pub struct QueryResult {}
