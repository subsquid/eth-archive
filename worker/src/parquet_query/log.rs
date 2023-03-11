use crate::parquet_metadata::{hash, LogRowGroupMetadata};
use crate::types::{LogQueryResult, MiniLogSelection};
use xorf::Filter;
use super::ParquetQuery;
use std::sync::Arc;
use crate::{Result, Error};
use std::{fs, io};
use arrow2::io::parquet;
use eth_archive_ingester::schema::log_schema;
use std::collections::{BTreeMap, BTreeSet, HashMap};

pub fn prune_log_queries_per_rg(
    rg_meta: &LogRowGroupMetadata,
    log_selections: &[MiniLogSelection],
) -> Vec<MiniLogSelection> {
    log_selections
        .iter()
        .filter_map(|log_selection| {
            let address: Vec<_> = log_selection
                .address
                .iter()
                .filter(|addr| rg_meta.address_filter.contains(&hash(addr.as_slice())))
                .cloned()
                .collect();

            if !log_selection.address.is_empty() && address.is_empty() {
                return None;
            }

            let mut new_selection = MiniLogSelection {
                address,
                topics: log_selection.topics.clone(),
            };

            if let Some(topic0) = log_selection.topics.get(0) {
                let pruned_topic0: Vec<_> = topic0
                    .iter()
                    .filter(|v| rg_meta.topic0_filter.contains(&hash(v.as_slice())))
                    .cloned()
                    .collect();

                if !topic0.is_empty() && pruned_topic0.is_empty() {
                    return None;
                }

                new_selection.topics[0] = pruned_topic0;
            }

            Some(new_selection)
        })
        .collect()
}

pub fn query_logs(query: Arc<ParquetQuery>, pruned_queries_per_rg: Vec<Vec<MiniLogSelection>>) -> Result<LogQueryResult> {
	let mut path = query.data_path.clone();
	path.push(query.dir_name.to_string());
	path.push("log.parquet");
	let file = fs::File::open(&path).map_err(Error::OpenParquetFile)?;
	let mut reader = io::BufReader::new(file);
	
	let metadata = parquet::read::read_metadata(&mut reader).map_err(Error::ReadParquet)?;
	
	let selected_fields = query.mini_query.field_selection.log.to_fields();
	
	let fields: Vec<_> = log_schema().fields.into_iter().filter(|field| {
		selected_fields.contains(field.name.as_str())
	}).collect();
	
	let mut query_result = LogQueryResult {
		logs: BTreeMap::new(),
		transactions: BTreeSet::new(),
		blocks: BTreeSet::new(),
	};
	
	for (rg_meta, log_queries) in metadata.row_groups.iter().zip(pruned_queries_per_rg.iter()) {
		if log_queries.is_empty() {
			continue;
		}

		let columns = parquet::read::read_columns_many(&mut reader, rg_meta, fields.clone(), None, None, None).map_err(Error::ReadParquet)?;
	
		for columns in columns {
			let columns: HashMap<String, _> = columns.into_iter().zip(fields.iter()).map(|(col, field)| {
				let col = col.map_err(Error::ReadParquet)?;
				Ok((field.name.to_owned(), col))
			}).collect::<Result<_>>()?;
			
			
		}
	}
	
	Ok(query_result)
}

