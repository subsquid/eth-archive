use crate::config::DataConfig;
use crate::db::DbHandle;
use crate::field_selection::FieldSelection;
use crate::range_map::RangeMap;
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

    pub fn height(&self) -> u32 {
        self.db.height()
    }

    pub async fn query(&self, query: Query) -> Result<Vec<u8>> {
        let query_start = Instant::now();

        let to_block = query
            .to_block
            .unwrap_or(query.from_block + self.config.default_block_range);

        if to_block == 0 || query.from_block > to_block {
            return Err(Error::InvalidBlockRange);
        }

        let to_block = cmp::min(to_block, query.from_block + self.config.max_block_range);

        let archive_height = self.db.height();

        let to_block = cmp::min(to_block, archive_height);

        let to_block = to_block + 1;

        let mut field_selection = None;
        for log in &query.logs {
            field_selection = FieldSelection::merge(field_selection, log.field_selection);
        }
        for tx in &query.transactions {
            field_selection = FieldSelection::merge(field_selection, tx.field_selection);
        }
        let mut field_selection = field_selection.ok_or(Error::NoFieldsSelected)?;

        let mut block_selection = field_selection.block.unwrap_or_default();
        let mut tx_selection = field_selection.transaction.unwrap_or_default();
        let mut log_selection = field_selection.log.unwrap_or_default();

        block_selection.number = Some(true);
        tx_selection.hash = Some(true);
        tx_selection.block_number = Some(true);
        tx_selection.transaction_index = Some(true);
        tx_selection.dest = Some(true);
        log_selection.block_number = Some(true);
        log_selection.transaction_index = Some(true);
        log_selection.address = Some(true);
        log_selection.topics = Some(true);

        field_selection.block = Some(block_selection);
        field_selection.transaction = Some(tx_selection);
        field_selection.log = Some(log_selection);

        let log_selection: Vec<MiniLogSelection> = query
            .logs
            .into_iter()
            .map(|log| MiniLogSelection {
                address: log.address,
                topics: log.topics,
            })
            .collect();

        let transaction_selection: Vec<MiniTransactionSelection> = query
            .transactions
            .into_iter()
            .map(|transaction| MiniTransactionSelection {
                address: transaction.address,
                sighash: transaction.sighash,
            })
            .collect();

        let (tx, mut rx): (mpsc::Sender<(QueryResult, _)>, _) = mpsc::channel(1);

        let serialize_thread = tokio::spawn(async move {
            let mut metrics = QueryMetrics::default();

            let mut bytes = br#"{"data":["#.to_vec();

            let mut comma = false;

            let mut next_block = query.from_block;

            while let Some((res, end)) = rx.recv().await {
                let mut data = Vec::new();

                let mut block_idxs = HashMap::new();
                let mut tx_idxs = HashMap::new();

                next_block = end;
                metrics += res.metrics;

                if res.data.is_empty() {
                    continue;
                }

                for row in res.data {
                    let block_number = row.block.number.unwrap().0;
                    let tx_hash = row.transaction.hash.clone().unwrap().0;

                    let block_idx = match block_idxs.get(&block_number) {
                        Some(block_idx) => *block_idx,
                        None => {
                            let block_idx = data.len();

                            data.push(BlockEntry {
                                block: row.block,
                                logs: Vec::new(),
                                transactions: Vec::new(),
                            });

                            block_idxs.insert(block_number, block_idx);

                            block_idx
                        }
                    };

                    if let std::collections::hash_map::Entry::Vacant(e) = tx_idxs.entry(tx_hash) {
                        e.insert(data[block_idx].transactions.len());
                        data[block_idx].transactions.push(row.transaction);
                    }

                    if let Some(log) = row.log {
                        data[block_idx].logs.push(log);
                    }
                }

                if comma {
                    bytes.push(b',');
                }
                comma = true;

                let start = Instant::now();

                serde_json::to_writer(&mut bytes, &data).unwrap();

                let elapsed = start.elapsed().as_millis();
                metrics.serialize_result += elapsed;
                metrics.total += elapsed;
            }

            let metrics = serde_json::to_string(&metrics).unwrap();

            let archive_height = match archive_height {
                0 => "null",
                _ => (archive_height - 1).to_string(),
            };

            bytes.extend_from_slice(
                format!(
                    r#"],"metrics":{},"archive_height":{},"nextBlock":{},"totalTime":{}}}"#,
                    metrics,
                    archive_height,
                    next_block,
                    query_start.elapsed().as_millis(),
                )
                .as_bytes(),
            );

            Ok(bytes)
        });

        let mut num_logs = 0;

        let start_time = Instant::now();

        let mut start = query.from_block;

        while start < to_block {
            let end = match self.parquet_state.read().await.log_ranges.get_next(start) {
                Some(end) => end,
                None => start + self.config.query_chunk_size,
            };

            let end = cmp::min(to_block, end);

            let res = self
                .query_impl(MiniQuery {
                    from_block: start,
                    to_block: end,
                    logs: log_selection.clone(),
                    transactions: transaction_selection.clone(),
                    field_selection,
                })
                .await?;

            num_logs += res.data.len();

            tx.send((res, end)).await.ok().unwrap();

            if num_logs > self.config.response_log_limit
                || start_time.elapsed().as_millis() > u128::from(self.config.query_time_limit_ms)
            {
                break;
            }

            start = end;
        }

        mem::drop(tx);

        let bytes = serialize_thread.await.unwrap()?;

        Ok(bytes)
    }

    async fn query_impl(&self, query: MiniQuery) -> Result<QueryResult> {
        let parquet_block_number = { self.parquet_state.read().await.parquet_block_number };

        if parquet_block_number >= query.to_block {
            let ctx = DataCtx {
                db: self.db.clone(),
                parquet_state: self.parquet_state.clone(),
                config: self.config.clone(),
            };

            tokio::task::spawn_blocking(move || ctx.query_parquet(query))
                .await
                .map_err(Error::TaskJoinError)?
        } else {
            self.query_sql(query).await
        }
    }

    async fn query_sql(&self, query: MiniQuery) -> Result<QueryResult> {
        let mut metrics = QueryMetrics::default();
        let mut data = vec![];

        if !query.logs.is_empty() {
            let start_time = Instant::now();
            let query = query.to_log_sql()?;
            let build_query = start_time.elapsed().as_millis();

            let logs = self
                .db
                .log_query(build_query, &query)
                .await
                .map_err(Error::SqlQuery)?;
            metrics += logs.metrics;
            for row in logs.data {
                data.push(row);
            }
        }

        if !query.transactions.is_empty() {
            let start_time = Instant::now();
            let query = query.to_tx_sql()?;
            let build_query = start_time.elapsed().as_millis();

            let transactions = self
                .db
                .tx_query(build_query, &query)
                .await
                .map_err(Error::SqlQuery)?;
            metrics += transactions.metrics;
            for row in transactions.data {
                data.push(row);
            }
        }

        Ok(QueryResult { data, metrics })
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

    fn query_transactions(&self, query: &MiniQuery) -> Result<QueryResult> {
        use polars::prelude::*;

        let start_time = Instant::now();

        let mut data_frame =
            self.setup_tx_pruned_frame(query.field_selection, query.from_block, query.to_block)?;

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
