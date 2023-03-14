use super::util::{define_cols, map_from_arrow};
use super::ParquetQuery;
use crate::parquet_metadata::LogRowGroupMetadata;
use crate::types::{LogQueryResult, MiniLogSelection, MiniQuery};
use crate::{Error, Result};
use arrayvec::ArrayVec;
use arrow2::array::{self, Array, BooleanArray, UInt32Array};
use arrow2::datatypes::Schema;
use arrow2::io::parquet;
use eth_archive_core::deserialize::{Address, Bytes, Bytes32, Index};
use eth_archive_core::hash::{HashMap, HashSet};
use eth_archive_core::types::ResponseLog;
use eth_archive_ingester::schema::log_schema;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::sync::Arc;

type BinaryArray = array::BinaryArray<i32>;

pub fn prune_log_queries_per_rg(
    rg_meta: &LogRowGroupMetadata,
    log_selections: &[MiniLogSelection],
) -> Vec<MiniLogSelection> {
    log_selections
        .iter()
        .filter_map(|log_selection| {
            let address = log_selection
                .address
                .iter()
                .filter(|addr| rg_meta.address_filter.contains(addr))
                .cloned()
                .collect::<HashSet<_>>();

            if !log_selection.address.is_empty() && address.is_empty() {
                return None;
            }

            match log_selection.topics.get(0) {
                Some(topic0) if !topic0.is_empty() => {
                    let pruned_topic0 = topic0
                        .iter()
                        .filter(|topic| rg_meta.topic0_filter.contains(topic))
                        .cloned()
                        .collect::<HashSet<_>>();

                    if pruned_topic0.is_empty() {
                        return None;
                    }

                    let mut topics = log_selection.topics.clone();
                    topics[0] = pruned_topic0;

                    Some(MiniLogSelection { address, topics })
                }
                _ => Some(MiniLogSelection {
                    address,
                    topics: log_selection.topics.clone(),
                }),
            }
        })
        .collect()
}

pub fn query_logs(
    query: Arc<ParquetQuery>,
    pruned_queries_per_rg: Vec<Vec<MiniLogSelection>>,
) -> Result<LogQueryResult> {
    let mut path = query.data_path.clone();
    path.push(query.dir_name.to_string());
    path.push("log.parquet");
    let mut file = fs::File::open(&path).map_err(Error::OpenParquetFile)?;

    let metadata = parquet::read::read_metadata(&mut file).map_err(Error::ReadParquet)?;

    let selected_fields = query.mini_query.field_selection.log.as_fields();

    let fields: Vec<_> = log_schema()
        .fields
        .into_iter()
        .filter(|field| selected_fields.contains(field.name.as_str()))
        .collect();

    let mut row_groups = Vec::new();
    let mut queries = Vec::new();

    for (rg_meta, log_queries) in metadata
        .row_groups
        .into_iter()
        .zip(pruned_queries_per_rg.iter())
    {
        if !log_queries.is_empty() {
            row_groups.push(rg_meta);
            queries.push(log_queries);
        }
    }

    let reader = parquet::read::FileReader::new(
        file,
        row_groups,
        Schema {
            fields: fields.clone(),
            metadata: Default::default(),
        },
        None,
        None,
        None,
    );

    let mut query_result = LogQueryResult {
        logs: BTreeMap::new(),
        transactions: BTreeSet::new(),
        blocks: BTreeSet::new(),
    };

    for (chunk, log_queries) in reader.zip(queries) {
        let chunk = chunk.map_err(Error::ReadParquet)?;

        let columns = chunk
            .into_arrays()
            .into_iter()
            .zip(fields.iter())
            .map(|(col, field)| (field.name.to_owned(), col))
            .collect::<HashMap<_, _>>();

        process_cols(&query.mini_query, log_queries, columns, &mut query_result);
    }

    Ok(query_result)
}

fn process_cols(
    query: &MiniQuery,
    log_queries: &[MiniLogSelection],
    mut columns: HashMap<String, Box<dyn Array>>,
    query_result: &mut LogQueryResult,
) {
    #[rustfmt::skip]
	define_cols!(
    	columns,
    	address, BinaryArray,
    	block_hash, BinaryArray,
    	block_number, UInt32Array,
    	data, BinaryArray,
    	log_index, UInt32Array,
    	removed, BooleanArray,
    	topic0, BinaryArray,
    	topic1, BinaryArray,
    	topic2, BinaryArray,
    	topic3, BinaryArray,
    	transaction_hash, BinaryArray,
    	transaction_index, UInt32Array
	);

    let len = block_number.as_ref().unwrap().len();

    for i in 0..len {
        let log = ResponseLog {
            address: map_from_arrow!(address, Address::new, i),
            block_hash: map_from_arrow!(block_hash, Bytes32::new, i),
            block_number: map_from_arrow!(block_number, Index, i),
            data: map_from_arrow!(data, Bytes::new, i),
            log_index: map_from_arrow!(log_index, Index, i),
            removed: removed.as_ref().and_then(|arr| arr.get(i)),
            topics: {
                let mut topics = ArrayVec::new();

                if let Some(Some(topic)) = topic0.as_ref().map(|arr| arr.get(i)) {
                    topics.push(Bytes32::new(topic));
                }

                if let Some(Some(topic)) = topic1.as_ref().map(|arr| arr.get(i)) {
                    topics.push(Bytes32::new(topic));
                }

                if let Some(Some(topic)) = topic2.as_ref().map(|arr| arr.get(i)) {
                    topics.push(Bytes32::new(topic));
                }

                if let Some(Some(topic)) = topic3.as_ref().map(|arr| arr.get(i)) {
                    topics.push(Bytes32::new(topic));
                }

                Some(topics)
            },
            transaction_hash: map_from_arrow!(transaction_hash, Bytes32::new, i),
            transaction_index: map_from_arrow!(transaction_index, Index, i),
        };

        let block_number = log.block_number.unwrap().0;
        let log_index = log.log_index.unwrap().0;
        let transaction_index = log.transaction_index.unwrap().0;

        if query.from_block > block_number || query.to_block <= block_number {
            continue;
        }

        if !MiniLogSelection::matches_log_impl(
            log_queries,
            log.address.as_ref().unwrap(),
            log.topics.as_ref().unwrap(),
        ) {
            continue;
        }

        query_result.logs.insert((block_number, log_index), log);
        query_result
            .transactions
            .insert((block_number, transaction_index));
        query_result.blocks.insert(block_number);
    }
}
