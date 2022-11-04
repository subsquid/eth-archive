use crate::config::DataConfig;
use crate::db::DbHandle;
use crate::field_selection::FieldSelection;
use crate::range_map::RangeMap;
use crate::serialize_task::SerializeTask;
use crate::types::{
    BlockEntry, MiniLogSelection, MiniQuery, MiniTransactionSelection, Query, Status,
};
use crate::{Error, Result};
use eth_archive_core::types::{
    QueryMetrics, QueryResult, ResponseBlock, ResponseLog, ResponseRow, ResponseTransaction,
};
use polars::prelude::*;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::{cmp, mem};
use tokio::fs;
use tokio::sync::{mpsc, RwLock};

pub struct DataCtx {
    config: DataConfig,
    db: Arc<DbHandle>,
}

impl DataCtx {
    pub async fn new(config: DataConfig) -> Result<Self> {
        let db = DbHandle::new().await?;
        let db = Arc::new(db);

        Ok(Self { config, db })
    }

    pub async fn query(self: Arc<Self>, query: Query) -> Result<Vec<u8>> {
        // to_block is supposed to be inclusive in api but exclusive in implementation
        // so we add one to it here
        let to_block = query.to_block.map(|a| a + 1);

        if let Some(to_block) = to_block {
            if to_block == 1 || query.from_block > to_block {
                return Err(Error::InvalidBlockRange);
            }
        }

        let field_selection = query.field_selection();

        let log_selection = query.log_selection();
        let tx_selection = query.tx_selection();

        let height = self.db.height();

        let inclusive_height = match height {
            0 => None,
            num => Some(num - 1),
        };

        let serialize_task = SerializeTask::new(
            query.from_block,
            self.config.max_resp_body_size,
            inclusive_height,
        );

        if query.from_block < height {
            if query.from_block < self.db.parquet_height() {
                // iter parquet idxs, prune and execute queries
            }

            match to_block {
                None | Some(to_block) if to_block <= height => {
                    // query db
                }
                _ => (),
            }
        }

        serialize_task.join().await
    }

    fn query_parquet(&self, query: MiniQuery) -> Result<QueryResult> {
        let mut metrics = QueryMetrics::default();
        let mut data = vec![];

        if !query.logs.is_empty() {
            let logs = self.query_logs(&query)?;
            metrics += logs.metrics;
            data.extend_from_slice(&logs.data);
        }

        if !query.transactions.is_empty() {
            let transactions = self.query_transactions(&query)?;
            metrics += transactions.metrics;
            data.extend_from_slice(&transactions.data);
        }

        Ok(QueryResult { data, metrics })
    }

    fn query_logs(&self, query: &MiniQuery) -> Result<QueryResult> {
        use polars::prelude::*;

        let start_time = Instant::now();

        let mut data_frame =
            self.setup_log_pruned_frame(query.field_selection, query.from_block, query.to_block)?;

        data_frame = data_frame.filter(
            col("log_block_number")
                .gt_eq(lit(query.from_block))
                .and(col("log_block_number").lt(lit(query.to_block))),
        );

        if !query.logs.is_empty() {
            let mut expr: Option<Expr> = None;

            for log in &query.logs {
                if let Some(inner_expr) = log.to_expr()? {
                    expr = match expr {
                        Some(expr) => Some(expr.or(inner_expr)),
                        None => Some(inner_expr),
                    };
                } else {
                    expr = None;
                    break;
                }
            }

            if let Some(expr) = expr {
                data_frame = data_frame.filter(expr);
            }
        }

        let build_query = start_time.elapsed().as_millis();

        let start_time = Instant::now();

        let result_frame = data_frame.collect().map_err(Error::ExecuteQuery)?;

        let run_query = start_time.elapsed().as_millis();

        let start_time = Instant::now();

        let data = response_rows_from_result_frame(result_frame)?;

        let serialize_result = start_time.elapsed().as_millis();

        Ok(QueryResult {
            data,
            metrics: QueryMetrics {
                build_query,
                run_query,
                serialize_result,
                total: build_query + run_query + serialize_result,
            },
        })
    }

    fn query_transactions(&self, query: &MiniQuery, lazy_frame: LazyFrame) -> Result<QueryResult> {
        use polars::prelude::*;

        let start_time = Instant::now();

        data_frame = data_frame.filter(
            col("tx_block_number")
                .gt_eq(lit(query.from_block))
                .and(col("tx_block_number").lt(lit(query.to_block))),
        );

        if !query.transactions.is_empty() {
            let mut expr: Option<Expr> = None;

            for tx in &query.transactions {
                if let Some(inner_expr) = tx.to_expr()? {
                    expr = match expr {
                        Some(expr) => Some(expr.or(inner_expr)),
                        None => Some(inner_expr),
                    };
                } else {
                    expr = None;
                    break;
                }
            }

            if let Some(expr) = expr {
                data_frame = data_frame.filter(expr);
            }
        }

        let build_query = start_time.elapsed().as_millis();

        let start_time = Instant::now();

        let result_frame = data_frame.collect().map_err(Error::ExecuteQuery)?;

        let run_query = start_time.elapsed().as_millis();

        let start_time = Instant::now();

        let data = tx_response_rows_from_result_frame(result_frame)?;

        let serialize_result = start_time.elapsed().as_millis();

        Ok(QueryResult {
            data,
            metrics: QueryMetrics {
                build_query,
                run_query,
                serialize_result,
                total: build_query + run_query + serialize_result,
            },
        })
    }
}

// defines columns using the result frame and a list of column names
macro_rules! define_cols {
    ($batch:expr, $schema:expr, $($name:ident),*) => {
        let columns = $batch.columns();
        $(
            let $name = $schema.get_full(stringify!($name)).map(|(i, _, _)| {
                columns.get(i).unwrap()
            });
        )*
    };
}

fn response_rows_from_result_frame(result_frame: DataFrame) -> Result<Vec<ResponseRow>> {
    use eth_archive_core::deserialize::{
        Address, BigInt, BloomFilterBytes, Bytes, Bytes32, Index, Nonce,
    };
    use polars::export::arrow::array::{self, BooleanArray, Int64Array, UInt32Array, UInt64Array};

    type BinaryArray = array::BinaryArray<i64>;

    let mut data = Vec::new();

    let schema = result_frame.schema();

    for batch in result_frame.iter_chunks() {
        define_cols!(
            batch,
            schema,
            block_hash,
            block_parent_hash,
            block_nonce,
            block_sha3_uncles,
            block_logs_bloom,
            block_transactions_root,
            block_state_root,
            block_receipts_root,
            block_miner,
            block_difficulty,
            block_total_difficulty,
            block_extra_data,
            block_size,
            block_gas_limit,
            block_gas_used,
            block_timestamp,
            tx_block_hash,
            tx_source,
            tx_gas,
            tx_gas_price,
            tx_hash,
            tx_input,
            tx_nonce,
            tx_dest,
            tx_value,
            tx_kind,
            tx_chain_id,
            tx_v,
            tx_r,
            tx_s,
            log_address,
            log_block_hash,
            log_block_number,
            log_data,
            log_log_index,
            log_removed,
            log_topic0,
            log_topic1,
            log_topic2,
            log_topic3,
            log_transaction_hash,
            log_transaction_index
        );

        let len = log_block_number.as_ref().unwrap().len();

        for i in 0..len {
            let response_row = ResponseRow {
                block: ResponseBlock {
                    number: log_block_number
                        .map(|arr| {
                            arr.as_any()
                                .downcast_ref::<UInt32Array>()
                                .unwrap()
                                .get(i)
                                .unwrap()
                        })
                        .map(Index),
                    hash: block_hash
                        .map(|arr| {
                            arr.as_any()
                                .downcast_ref::<BinaryArray>()
                                .unwrap()
                                .get(i)
                                .unwrap()
                        })
                        .map(Bytes32::new),
                    parent_hash: block_parent_hash
                        .map(|arr| {
                            arr.as_any()
                                .downcast_ref::<BinaryArray>()
                                .unwrap()
                                .get(i)
                                .unwrap()
                        })
                        .map(Bytes32::new),
                    nonce: block_nonce
                        .map(|arr| {
                            arr.as_any()
                                .downcast_ref::<UInt64Array>()
                                .unwrap()
                                .get(i)
                                .unwrap()
                        })
                        .map(Nonce),
                    sha3_uncles: block_sha3_uncles
                        .map(|arr| {
                            arr.as_any()
                                .downcast_ref::<BinaryArray>()
                                .unwrap()
                                .get(i)
                                .unwrap()
                        })
                        .map(Bytes32::new),
                    logs_bloom: block_logs_bloom
                        .map(|arr| {
                            arr.as_any()
                                .downcast_ref::<BinaryArray>()
                                .unwrap()
                                .get(i)
                                .unwrap()
                        })
                        .map(BloomFilterBytes::new),
                    transactions_root: block_transactions_root
                        .map(|arr| {
                            arr.as_any()
                                .downcast_ref::<BinaryArray>()
                                .unwrap()
                                .get(i)
                                .unwrap()
                        })
                        .map(Bytes32::new),
                    state_root: block_state_root
                        .map(|arr| {
                            arr.as_any()
                                .downcast_ref::<BinaryArray>()
                                .unwrap()
                                .get(i)
                                .unwrap()
                        })
                        .map(Bytes32::new),
                    receipts_root: block_receipts_root
                        .map(|arr| {
                            arr.as_any()
                                .downcast_ref::<BinaryArray>()
                                .unwrap()
                                .get(i)
                                .unwrap()
                        })
                        .map(Bytes32::new),
                    miner: block_miner
                        .map(|arr| {
                            arr.as_any()
                                .downcast_ref::<BinaryArray>()
                                .unwrap()
                                .get(i)
                                .unwrap()
                        })
                        .map(Address::new),
                    difficulty: block_difficulty
                        .map(|arr| {
                            arr.as_any()
                                .downcast_ref::<BinaryArray>()
                                .unwrap()
                                .get(i)
                                .unwrap()
                        })
                        .map(Bytes::new),
                    total_difficulty: block_total_difficulty
                        .map(|arr| {
                            arr.as_any()
                                .downcast_ref::<BinaryArray>()
                                .unwrap()
                                .get(i)
                                .unwrap()
                        })
                        .map(Bytes::new),
                    extra_data: block_extra_data
                        .map(|arr| {
                            arr.as_any()
                                .downcast_ref::<BinaryArray>()
                                .unwrap()
                                .get(i)
                                .unwrap()
                        })
                        .map(Bytes::new),
                    size: block_size
                        .map(|arr| {
                            arr.as_any()
                                .downcast_ref::<Int64Array>()
                                .unwrap()
                                .get(i)
                                .unwrap()
                        })
                        .map(BigInt),
                    gas_limit: block_gas_limit
                        .map(|arr| {
                            arr.as_any()
                                .downcast_ref::<BinaryArray>()
                                .unwrap()
                                .get(i)
                                .unwrap()
                        })
                        .map(Bytes::new),
                    gas_used: block_gas_used
                        .map(|arr| {
                            arr.as_any()
                                .downcast_ref::<BinaryArray>()
                                .unwrap()
                                .get(i)
                                .unwrap()
                        })
                        .map(Bytes::new),
                    timestamp: block_timestamp
                        .map(|arr| {
                            arr.as_any()
                                .downcast_ref::<Int64Array>()
                                .unwrap()
                                .get(i)
                                .unwrap()
                        })
                        .map(BigInt),
                },
                transaction: ResponseTransaction {
                    block_hash: tx_block_hash
                        .map(|arr| {
                            arr.as_any()
                                .downcast_ref::<BinaryArray>()
                                .unwrap()
                                .get(i)
                                .unwrap()
                        })
                        .map(Bytes32::new),
                    block_number: log_block_number
                        .map(|arr| {
                            arr.as_any()
                                .downcast_ref::<UInt32Array>()
                                .unwrap()
                                .get(i)
                                .unwrap()
                        })
                        .map(Index),
                    source: tx_source
                        .map(|arr| {
                            arr.as_any()
                                .downcast_ref::<BinaryArray>()
                                .unwrap()
                                .get(i)
                                .unwrap()
                        })
                        .map(Address::new),
                    gas: tx_gas
                        .map(|arr| {
                            arr.as_any()
                                .downcast_ref::<Int64Array>()
                                .unwrap()
                                .get(i)
                                .unwrap()
                        })
                        .map(BigInt),
                    gas_price: tx_gas_price
                        .map(|arr| {
                            arr.as_any()
                                .downcast_ref::<Int64Array>()
                                .unwrap()
                                .get(i)
                                .unwrap()
                        })
                        .map(BigInt),
                    hash: tx_hash
                        .map(|arr| {
                            arr.as_any()
                                .downcast_ref::<BinaryArray>()
                                .unwrap()
                                .get(i)
                                .unwrap()
                        })
                        .map(Bytes32::new),
                    input: tx_input
                        .map(|arr| {
                            arr.as_any()
                                .downcast_ref::<BinaryArray>()
                                .unwrap()
                                .get(i)
                                .unwrap()
                        })
                        .map(Bytes::new),
                    nonce: tx_nonce
                        .map(|arr| {
                            arr.as_any()
                                .downcast_ref::<UInt64Array>()
                                .unwrap()
                                .get(i)
                                .unwrap()
                        })
                        .map(Nonce),
                    dest: match tx_dest
                        .map(|arr| arr.as_any().downcast_ref::<BinaryArray>().unwrap().get(i))
                    {
                        Some(Some(addr)) => Some(Address::new(addr)),
                        _ => None,
                    },
                    transaction_index: log_transaction_index
                        .map(|arr| {
                            arr.as_any()
                                .downcast_ref::<UInt32Array>()
                                .unwrap()
                                .get(i)
                                .unwrap()
                        })
                        .map(Index),
                    value: tx_value
                        .map(|arr| {
                            arr.as_any()
                                .downcast_ref::<BinaryArray>()
                                .unwrap()
                                .get(i)
                                .unwrap()
                        })
                        .map(Bytes::new),
                    kind: tx_kind
                        .map(|arr| {
                            arr.as_any()
                                .downcast_ref::<UInt32Array>()
                                .unwrap()
                                .get(i)
                                .unwrap()
                        })
                        .map(Index),
                    chain_id: tx_chain_id
                        .map(|arr| {
                            arr.as_any()
                                .downcast_ref::<UInt32Array>()
                                .unwrap()
                                .get(i)
                                .unwrap()
                        })
                        .map(Index),
                    v: tx_v
                        .map(|arr| {
                            arr.as_any()
                                .downcast_ref::<Int64Array>()
                                .unwrap()
                                .get(i)
                                .unwrap()
                        })
                        .map(BigInt),
                    r: tx_r
                        .map(|arr| {
                            arr.as_any()
                                .downcast_ref::<BinaryArray>()
                                .unwrap()
                                .get(i)
                                .unwrap()
                        })
                        .map(Bytes::new),
                    s: tx_s
                        .map(|arr| {
                            arr.as_any()
                                .downcast_ref::<BinaryArray>()
                                .unwrap()
                                .get(i)
                                .unwrap()
                        })
                        .map(Bytes::new),
                },
                log: Some(ResponseLog {
                    address: log_address
                        .map(|arr| {
                            arr.as_any()
                                .downcast_ref::<BinaryArray>()
                                .unwrap()
                                .get(i)
                                .unwrap()
                        })
                        .map(Address::new),
                    block_hash: log_block_hash
                        .map(|arr| {
                            arr.as_any()
                                .downcast_ref::<BinaryArray>()
                                .unwrap()
                                .get(i)
                                .unwrap()
                        })
                        .map(Bytes32::new),
                    block_number: log_block_number
                        .map(|arr| {
                            arr.as_any()
                                .downcast_ref::<UInt32Array>()
                                .unwrap()
                                .get(i)
                                .unwrap()
                        })
                        .map(Index),
                    data: log_data
                        .map(|arr| {
                            arr.as_any()
                                .downcast_ref::<BinaryArray>()
                                .unwrap()
                                .get(i)
                                .unwrap()
                        })
                        .map(Bytes::new),
                    log_index: log_log_index
                        .map(|arr| {
                            arr.as_any()
                                .downcast_ref::<UInt32Array>()
                                .unwrap()
                                .get(i)
                                .unwrap()
                        })
                        .map(Index),
                    removed: log_removed.map(|arr| {
                        arr.as_any()
                            .downcast_ref::<BooleanArray>()
                            .unwrap()
                            .get(i)
                            .unwrap()
                    }),
                    topics: {
                        let mut topics = vec![];

                        if let Some(Some(topic)) = log_topic0
                            .map(|arr| arr.as_any().downcast_ref::<BinaryArray>().unwrap().get(i))
                        {
                            topics.push(Bytes32::new(topic));
                        }

                        if let Some(Some(topic)) = log_topic1
                            .map(|arr| arr.as_any().downcast_ref::<BinaryArray>().unwrap().get(i))
                        {
                            topics.push(Bytes32::new(topic));
                        }

                        if let Some(Some(topic)) = log_topic2
                            .map(|arr| arr.as_any().downcast_ref::<BinaryArray>().unwrap().get(i))
                        {
                            topics.push(Bytes32::new(topic));
                        }

                        if let Some(Some(topic)) = log_topic3
                            .map(|arr| arr.as_any().downcast_ref::<BinaryArray>().unwrap().get(i))
                        {
                            topics.push(Bytes32::new(topic));
                        }

                        Some(topics)
                    },
                    transaction_hash: log_transaction_hash
                        .map(|arr| {
                            arr.as_any()
                                .downcast_ref::<BinaryArray>()
                                .unwrap()
                                .get(i)
                                .unwrap()
                        })
                        .map(Bytes32::new),
                    transaction_index: log_transaction_index
                        .map(|arr| {
                            arr.as_any()
                                .downcast_ref::<UInt32Array>()
                                .unwrap()
                                .get(i)
                                .unwrap()
                        })
                        .map(Index),
                }),
            };

            data.push(response_row);
        }
    }

    Ok(data)
}

fn tx_response_rows_from_result_frame(result_frame: DataFrame) -> Result<Vec<ResponseRow>> {
    use eth_archive_core::deserialize::{
        Address, BigInt, BloomFilterBytes, Bytes, Bytes32, Index, Nonce,
    };
    use polars::export::arrow::array::{self, Int64Array, UInt32Array, UInt64Array};

    type BinaryArray = array::BinaryArray<i64>;

    let mut data = Vec::new();

    let schema = result_frame.schema();

    for batch in result_frame.iter_chunks() {
        define_cols!(
            batch,
            schema,
            block_hash,
            block_parent_hash,
            block_nonce,
            block_sha3_uncles,
            block_logs_bloom,
            block_transactions_root,
            block_state_root,
            block_receipts_root,
            block_miner,
            block_difficulty,
            block_total_difficulty,
            block_extra_data,
            block_size,
            block_gas_limit,
            block_gas_used,
            block_timestamp,
            tx_block_hash,
            tx_block_number,
            tx_source,
            tx_gas,
            tx_gas_price,
            tx_hash,
            tx_input,
            tx_nonce,
            tx_dest,
            tx_transaction_index,
            tx_value,
            tx_kind,
            tx_chain_id,
            tx_v,
            tx_r,
            tx_s
        );

        let len = tx_block_number.as_ref().unwrap().len();

        for i in 0..len {
            let response_row = ResponseRow {
                block: ResponseBlock {
                    number: tx_block_number
                        .map(|arr| {
                            arr.as_any()
                                .downcast_ref::<UInt32Array>()
                                .unwrap()
                                .get(i)
                                .unwrap()
                        })
                        .map(Index),
                    hash: block_hash
                        .map(|arr| {
                            arr.as_any()
                                .downcast_ref::<BinaryArray>()
                                .unwrap()
                                .get(i)
                                .unwrap()
                        })
                        .map(Bytes32::new),
                    parent_hash: block_parent_hash
                        .map(|arr| {
                            arr.as_any()
                                .downcast_ref::<BinaryArray>()
                                .unwrap()
                                .get(i)
                                .unwrap()
                        })
                        .map(Bytes32::new),
                    nonce: block_nonce
                        .map(|arr| {
                            arr.as_any()
                                .downcast_ref::<UInt64Array>()
                                .unwrap()
                                .get(i)
                                .unwrap()
                        })
                        .map(Nonce),
                    sha3_uncles: block_sha3_uncles
                        .map(|arr| {
                            arr.as_any()
                                .downcast_ref::<BinaryArray>()
                                .unwrap()
                                .get(i)
                                .unwrap()
                        })
                        .map(Bytes32::new),
                    logs_bloom: block_logs_bloom
                        .map(|arr| {
                            arr.as_any()
                                .downcast_ref::<BinaryArray>()
                                .unwrap()
                                .get(i)
                                .unwrap()
                        })
                        .map(BloomFilterBytes::new),
                    transactions_root: block_transactions_root
                        .map(|arr| {
                            arr.as_any()
                                .downcast_ref::<BinaryArray>()
                                .unwrap()
                                .get(i)
                                .unwrap()
                        })
                        .map(Bytes32::new),
                    state_root: block_state_root
                        .map(|arr| {
                            arr.as_any()
                                .downcast_ref::<BinaryArray>()
                                .unwrap()
                                .get(i)
                                .unwrap()
                        })
                        .map(Bytes32::new),
                    receipts_root: block_receipts_root
                        .map(|arr| {
                            arr.as_any()
                                .downcast_ref::<BinaryArray>()
                                .unwrap()
                                .get(i)
                                .unwrap()
                        })
                        .map(Bytes32::new),
                    miner: block_miner
                        .map(|arr| {
                            arr.as_any()
                                .downcast_ref::<BinaryArray>()
                                .unwrap()
                                .get(i)
                                .unwrap()
                        })
                        .map(Address::new),
                    difficulty: block_difficulty
                        .map(|arr| {
                            arr.as_any()
                                .downcast_ref::<BinaryArray>()
                                .unwrap()
                                .get(i)
                                .unwrap()
                        })
                        .map(Bytes::new),
                    total_difficulty: block_total_difficulty
                        .map(|arr| {
                            arr.as_any()
                                .downcast_ref::<BinaryArray>()
                                .unwrap()
                                .get(i)
                                .unwrap()
                        })
                        .map(Bytes::new),
                    extra_data: block_extra_data
                        .map(|arr| {
                            arr.as_any()
                                .downcast_ref::<BinaryArray>()
                                .unwrap()
                                .get(i)
                                .unwrap()
                        })
                        .map(Bytes::new),
                    size: block_size
                        .map(|arr| {
                            arr.as_any()
                                .downcast_ref::<Int64Array>()
                                .unwrap()
                                .get(i)
                                .unwrap()
                        })
                        .map(BigInt),
                    gas_limit: block_gas_limit
                        .map(|arr| {
                            arr.as_any()
                                .downcast_ref::<BinaryArray>()
                                .unwrap()
                                .get(i)
                                .unwrap()
                        })
                        .map(Bytes::new),
                    gas_used: block_gas_used
                        .map(|arr| {
                            arr.as_any()
                                .downcast_ref::<BinaryArray>()
                                .unwrap()
                                .get(i)
                                .unwrap()
                        })
                        .map(Bytes::new),
                    timestamp: block_timestamp
                        .map(|arr| {
                            arr.as_any()
                                .downcast_ref::<Int64Array>()
                                .unwrap()
                                .get(i)
                                .unwrap()
                        })
                        .map(BigInt),
                },
                transaction: ResponseTransaction {
                    block_hash: tx_block_hash
                        .map(|arr| {
                            arr.as_any()
                                .downcast_ref::<BinaryArray>()
                                .unwrap()
                                .get(i)
                                .unwrap()
                        })
                        .map(Bytes32::new),
                    block_number: tx_block_number
                        .map(|arr| {
                            arr.as_any()
                                .downcast_ref::<UInt32Array>()
                                .unwrap()
                                .get(i)
                                .unwrap()
                        })
                        .map(Index),
                    source: tx_source
                        .map(|arr| {
                            arr.as_any()
                                .downcast_ref::<BinaryArray>()
                                .unwrap()
                                .get(i)
                                .unwrap()
                        })
                        .map(Address::new),
                    gas: tx_gas
                        .map(|arr| {
                            arr.as_any()
                                .downcast_ref::<Int64Array>()
                                .unwrap()
                                .get(i)
                                .unwrap()
                        })
                        .map(BigInt),
                    gas_price: tx_gas_price
                        .map(|arr| {
                            arr.as_any()
                                .downcast_ref::<Int64Array>()
                                .unwrap()
                                .get(i)
                                .unwrap()
                        })
                        .map(BigInt),
                    hash: tx_hash
                        .map(|arr| {
                            arr.as_any()
                                .downcast_ref::<BinaryArray>()
                                .unwrap()
                                .get(i)
                                .unwrap()
                        })
                        .map(Bytes32::new),
                    input: tx_input
                        .map(|arr| {
                            arr.as_any()
                                .downcast_ref::<BinaryArray>()
                                .unwrap()
                                .get(i)
                                .unwrap()
                        })
                        .map(Bytes::new),
                    nonce: tx_nonce
                        .map(|arr| {
                            arr.as_any()
                                .downcast_ref::<UInt64Array>()
                                .unwrap()
                                .get(i)
                                .unwrap()
                        })
                        .map(Nonce),
                    dest: match tx_dest
                        .map(|arr| arr.as_any().downcast_ref::<BinaryArray>().unwrap().get(i))
                    {
                        Some(Some(addr)) => Some(Address::new(addr)),
                        _ => None,
                    },
                    transaction_index: tx_transaction_index
                        .map(|arr| {
                            arr.as_any()
                                .downcast_ref::<UInt32Array>()
                                .unwrap()
                                .get(i)
                                .unwrap()
                        })
                        .map(Index),
                    value: tx_value
                        .map(|arr| {
                            arr.as_any()
                                .downcast_ref::<BinaryArray>()
                                .unwrap()
                                .get(i)
                                .unwrap()
                        })
                        .map(Bytes::new),
                    kind: tx_kind
                        .map(|arr| {
                            arr.as_any()
                                .downcast_ref::<UInt32Array>()
                                .unwrap()
                                .get(i)
                                .unwrap()
                        })
                        .map(Index),
                    chain_id: tx_chain_id
                        .map(|arr| {
                            arr.as_any()
                                .downcast_ref::<UInt32Array>()
                                .unwrap()
                                .get(i)
                                .unwrap()
                        })
                        .map(Index),
                    v: tx_v
                        .map(|arr| {
                            arr.as_any()
                                .downcast_ref::<Int64Array>()
                                .unwrap()
                                .get(i)
                                .unwrap()
                        })
                        .map(BigInt),
                    r: tx_r
                        .map(|arr| {
                            arr.as_any()
                                .downcast_ref::<BinaryArray>()
                                .unwrap()
                                .get(i)
                                .unwrap()
                        })
                        .map(Bytes::new),
                    s: tx_s
                        .map(|arr| {
                            arr.as_any()
                                .downcast_ref::<BinaryArray>()
                                .unwrap()
                                .get(i)
                                .unwrap()
                        })
                        .map(Bytes::new),
                },
                log: None,
            };

            data.push(response_row);
        }
    }

    Ok(data)
}
