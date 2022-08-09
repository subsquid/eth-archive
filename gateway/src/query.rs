#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct QueryLogs {
    from_block: u64,
    to_block: u64,
    addresses: Vec<AddressQuery>,
    field_selection: LogFieldSelection,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct AddressQuery {
    address: String,
    topics: [Option<Vec<String>>; 4],
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
