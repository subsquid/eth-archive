use crate::config::Config;
use crate::db::DbHandle;
use crate::db_writer::DbWriter;
use crate::field_selection::FieldSelection;
use crate::serialize_task::SerializeTask;
use crate::types::{MiniLogSelection, MiniQuery, MiniTransactionSelection, Query};
use crate::{Error, Result};
use eth_archive_core::dir_name::DirName;
use eth_archive_core::eth_client::EthClient;
use eth_archive_core::ingest_metrics::IngestMetrics;
use eth_archive_core::rayon_async;
use eth_archive_core::retry::Retry;
use eth_archive_core::types::{
    BlockRange, QueryMetrics, QueryResult, ResponseBlock, ResponseLog, ResponseRow,
    ResponseTransaction,
};
use futures::pin_mut;
use futures::stream::StreamExt;
use polars::prelude::*;
use std::cmp;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub struct DataCtx {
    config: Config,
    db: Arc<DbHandle>,
}

impl DataCtx {
    pub async fn new(config: Config, ingest_metrics: Arc<IngestMetrics>) -> Result<Self> {
        let db = DbHandle::new(&config.db_path, ingest_metrics.clone()).await?;
        let db = Arc::new(db);

        let db_writer = DbWriter::new(db.clone(), &config.data_path, config.min_hot_block_range);
        let db_writer = Arc::new(db_writer);

        let retry = Retry::new(config.retry);
        let eth_client = EthClient::new(config.ingest.clone(), retry, ingest_metrics.clone())
            .map_err(Error::CreateEthClient)?;
        let eth_client = Arc::new(eth_client);

        tokio::spawn({
            let db_writer = db_writer.clone();
            let db_height = db.db_height();

            async move {
                let best_block = eth_client.clone().get_best_block().await.unwrap();
                let mut start = if best_block > config.min_hot_block_range {
                    best_block - config.min_hot_block_range
                } else {
                    0
                };

                if db_height > start {
                    start = db_height;
                }

                let batches = eth_client.clone().stream_batches(Some(start), None);
                pin_mut!(batches);

                while let Some(res) = batches.next().await {
                    let data = res.unwrap();
                    db_writer.write_batches(data).await;
                }
            }
        });

        tokio::spawn({
            let start = db.parquet_height();
            let data_path = config.data_path.clone();

            async move {
                let mut next_start = start;

                loop {
                    tokio::time::sleep(Duration::from_secs(5)).await;

                    let dir_names = DirName::find_sorted(&data_path, next_start).await.unwrap();

                    for dir_name in dir_names {
                        db_writer.register_parquet_folder(dir_name).await;
                        next_start = dir_name.range.to;
                    }
                }
            }
        });

        Ok(Self { config, db })
    }

    pub fn inclusive_height(&self) -> Option<u32> {
        match self.db.height() {
            0 => None,
            num => Some(num - 1),
        }
    }

    pub async fn query(self: Arc<Self>, query: Query) -> Result<Vec<u8>> {
        // to_block is supposed to be inclusive in api but exclusive in implementation
        // so we add one to it here
        let to_block = query.to_block.map(|a| a + 1);

        if let Some(to_block) = to_block {
            if query.from_block >= to_block {
                return Err(Error::InvalidBlockRange);
            }
        }

        let height = self.db.height();

        let inclusive_height = match height {
            0 => None,
            num => Some(num - 1),
        };

        let serialize_task = SerializeTask::new(
            query.from_block,
            self.config.max_resp_body_size,
            self.config.resp_time_limit,
            inclusive_height,
        );

        let query_task = rayon_async::spawn(move || {
            if query.from_block >= height {
                return Ok(serialize_task);
            }

            let parquet_height = self.db.parquet_height();

            let field_selection = query.field_selection()?;

            if query.from_block < parquet_height {
                for res in self.db.iter_parquet_idxs(query.from_block, to_block)? {
                    let (dir_name, parquet_idx) = res?;

                    let logs = query
                        .logs
                        .iter()
                        .filter_map(|log_selection| {
                            let address = match &log_selection.address {
                                Some(address) => address,
                                None => {
                                    return Some(MiniLogSelection {
                                        address: log_selection.address.clone(),
                                        topics: log_selection.topics.clone(),
                                    });
                                }
                            };

                            let address = address
                                .iter()
                                .filter(|addr| parquet_idx.log_addr_filter.contains(addr))
                                .cloned()
                                .collect::<Vec<_>>();

                            if !address.is_empty() {
                                Some(MiniLogSelection {
                                    address: Some(address),
                                    topics: log_selection.topics.clone(),
                                })
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<_>>();

                    let transactions = query
                        .transactions
                        .iter()
                        .filter_map(|tx_selection| {
                            let address = match &tx_selection.address {
                                Some(address) => address,
                                None => {
                                    return Some(MiniTransactionSelection {
                                        address: tx_selection.address.clone(),
                                        sighash: tx_selection.sighash.clone(),
                                    });
                                }
                            };

                            let address = address
                                .iter()
                                .filter(|addr| parquet_idx.tx_addr_filter.contains(addr))
                                .cloned()
                                .collect::<Vec<_>>();

                            if !address.is_empty() {
                                Some(MiniTransactionSelection {
                                    address: Some(address),
                                    sighash: tx_selection.sighash.clone(),
                                })
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<_>>();

                    let from_block = cmp::max(dir_name.range.from, query.from_block);
                    let to_block = match to_block {
                        Some(to_block) => cmp::min(dir_name.range.to, to_block),
                        None => dir_name.range.to,
                    };

                    let mini_query = MiniQuery {
                        from_block,
                        to_block,
                        logs,
                        transactions,
                        field_selection,
                    };

                    let block_range = BlockRange {
                        from: from_block,
                        to: to_block,
                    };

                    if serialize_task.is_closed() {
                        return Ok(serialize_task);
                    }

                    let res = self.query_parquet(dir_name, mini_query)?;

                    if !serialize_task.send((res, block_range)) {
                        return Ok(serialize_task);
                    }
                }
            }

            let from_block = cmp::max(query.from_block, parquet_height);

            if from_block < height {
                let to_block = match to_block {
                    Some(to_block) => cmp::min(height, to_block),
                    None => height,
                };

                let step = usize::try_from(self.config.db_query_batch_size).unwrap();
                for start in (from_block..to_block).step_by(step) {
                    let end = cmp::min(to_block, start + self.config.db_query_batch_size);

                    let mini_query = MiniQuery {
                        from_block: start,
                        to_block: end,
                        logs: query.log_selection(),
                        transactions: query.tx_selection(),
                        field_selection,
                    };

                    if serialize_task.is_closed() {
                        return Ok(serialize_task);
                    }

                    let res = self.db.query(mini_query)?;

                    let block_range = BlockRange {
                        from: start,
                        to: end,
                    };

                    if !serialize_task.send((res, block_range)) {
                        return Ok(serialize_task);
                    }
                }
            }

            Ok(serialize_task)
        });

        // run query task to end, then run serialization task to end
        query_task.await?.join().await
    }

    fn query_parquet(&self, dir_name: DirName, query: MiniQuery) -> Result<QueryResult> {
        let mut metrics = QueryMetrics::default();
        let mut data = vec![];

        if !query.logs.is_empty() {
            let logs = self.query_logs(
                &query,
                self.open_log_lazy_frame(dir_name, query.field_selection)?,
            )?;
            metrics += logs.metrics;
            data.extend_from_slice(&logs.data);
        }

        if !query.transactions.is_empty() {
            let transactions = self.query_transactions(
                &query,
                self.open_tx_lazy_frame(dir_name, query.field_selection)?,
            )?;
            metrics += transactions.metrics;
            data.extend_from_slice(&transactions.data);
        }

        Ok(QueryResult { data, metrics })
    }

    fn query_logs(&self, query: &MiniQuery, mut lazy_frame: LazyFrame) -> Result<QueryResult> {
        use polars::prelude::*;

        let start_time = Instant::now();

        lazy_frame = lazy_frame.filter(
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
                lazy_frame = lazy_frame.filter(expr);
            }
        }

        let build_query = start_time.elapsed().as_millis();

        let start_time = Instant::now();

        let result_frame = lazy_frame.collect().map_err(Error::ExecuteQuery)?;

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

    fn query_transactions(
        &self,
        query: &MiniQuery,
        mut lazy_frame: LazyFrame,
    ) -> Result<QueryResult> {
        use polars::prelude::*;

        let start_time = Instant::now();

        lazy_frame = lazy_frame.filter(
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
                lazy_frame = lazy_frame.filter(expr);
            }
        }

        let build_query = start_time.elapsed().as_millis();

        let start_time = Instant::now();

        let result_frame = lazy_frame.collect().map_err(Error::ExecuteQuery)?;

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

    fn open_log_lazy_frame(
        &self,
        dir_name: DirName,
        field_selection: FieldSelection,
    ) -> Result<LazyFrame> {
        let mut path = self.config.data_path.clone();
        path.push(dir_name.to_string());

        let blocks = {
            let mut path = path.clone();
            path.push("block.parquet");

            LazyFrame::scan_parquet(&path, Default::default()).map_err(Error::ScanParquet)?
        };

        let transactions = {
            let mut path = path.clone();
            path.push("tx.parquet");

            LazyFrame::scan_parquet(&path, Default::default()).map_err(Error::ScanParquet)?
        };

        let logs = {
            let mut path = path.clone();
            path.push("log.parquet");

            LazyFrame::scan_parquet(&path, Default::default()).map_err(Error::ScanParquet)?
        };

        let blocks = blocks.select(field_selection.block.unwrap().to_cols());
        let transactions = transactions.select(field_selection.transaction.unwrap().to_cols());
        let logs = logs.select(field_selection.log.unwrap().to_cols());

        let lazy_frame = logs
            .join(
                blocks,
                &[col("log_block_number")],
                &[col("block_number")],
                JoinType::Inner,
            )
            .join(
                transactions,
                &[col("log_block_number"), col("log_transaction_index")],
                &[col("tx_block_number"), col("tx_transaction_index")],
                JoinType::Inner,
            );

        Ok(lazy_frame)
    }

    fn open_tx_lazy_frame(
        &self,
        dir_name: DirName,
        field_selection: FieldSelection,
    ) -> Result<LazyFrame> {
        let mut path = self.config.data_path.clone();
        path.push(dir_name.to_string());

        let blocks = {
            let mut path = path.clone();
            path.push("block.parquet");

            LazyFrame::scan_parquet(&path, Default::default()).map_err(Error::ScanParquet)?
        };

        let transactions = {
            let mut path = path.clone();
            path.push("tx.parquet");

            LazyFrame::scan_parquet(&path, Default::default()).map_err(Error::ScanParquet)?
        };

        let blocks = blocks.select(field_selection.block.unwrap().to_cols());
        let mut transaction_cols = field_selection.transaction.unwrap().to_cols();
        let sighash_col = col("sighash").prefix("tx_");
        transaction_cols.push(sighash_col);
        let transactions = transactions.select(transaction_cols);

        let lazy_frame = transactions.join(
            blocks,
            &[col("tx_block_number")],
            &[col("block_number")],
            JoinType::Inner,
        );

        Ok(lazy_frame)
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
                    chain_id: match tx_chain_id
                        .map(|arr| arr.as_any().downcast_ref::<UInt32Array>().unwrap().get(i))
                    {
                        Some(Some(chain_id)) => Some(Index(chain_id)),
                        _ => None,
                    },
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
                    chain_id: match tx_chain_id
                        .map(|arr| arr.as_any().downcast_ref::<UInt32Array>().unwrap().get(i))
                    {
                        Some(Some(chain_id)) => Some(Index(chain_id)),
                        _ => None,
                    },
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
