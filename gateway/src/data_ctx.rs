use crate::config::DataConfig;
use crate::field_selection::FieldSelection;
use crate::range_map::RangeMap;
use crate::types::{
    BlockEntry, MiniLogSelection, MiniQuery, MiniTransactionSelection, Query, Status,
};
use crate::{Error, Result};
use eth_archive_core::db::DbHandle;
use eth_archive_core::types::{
    QueryMetrics, QueryResult, ResponseBlock, ResponseLog, ResponseRow, ResponseTransaction,
};
use polars::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use std::{cmp, mem};
use tokio::fs;
use tokio::sync::{mpsc, RwLock};

pub struct DataCtx {
    db: Arc<DbHandle>,
    config: DataConfig,
    parquet_state: Arc<RwLock<ParquetState>>,
}

#[derive(Debug)]
struct ParquetState {
    parquet_block_number: u32,
    block_ranges: RangeMap,
    tx_ranges: RangeMap,
    log_ranges: RangeMap,
}

impl DataCtx {
    pub async fn new(db: Arc<DbHandle>, config: DataConfig) -> Result<Self> {
        log::info!("setting up datafusion context...");

        let parquet_state = Self::setup_parquet_state(&config).await?;
        let parquet_state = Arc::new(RwLock::new(parquet_state));

        {
            let config = config.clone();
            let parquet_state = parquet_state.clone();
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(std::time::Duration::from_secs(
                    config.parquet_state_refresh_interval_secs,
                ));

                loop {
                    interval.tick().await;

                    log::info!("updating parquet state...");

                    let start_time = Instant::now();

                    let new_parquet_state = match Self::setup_parquet_state(&config).await {
                        Ok(new_parquet_state) => new_parquet_state,
                        Err(e) => {
                            log::error!("failed to update parquet state:\n{}", e);
                            continue;
                        }
                    };

                    let mut parquet_state = parquet_state.write().await;

                    *parquet_state = new_parquet_state;

                    log::info!(
                        "updated parquet state in {}ms",
                        start_time.elapsed().as_millis()
                    );
                }
            });
        }

        Ok(Self {
            db,
            config,
            parquet_state,
        })
    }

    async fn setup_parquet_state(config: &DataConfig) -> Result<ParquetState> {
        log::info!("collecting block range info for parquet (block header) files...");
        let (block_ranges, blk_num) = Self::get_block_ranges("block", &config.blocks_path).await?;
        log::info!("collecting block range info for parquet (transaction) files...");
        let (tx_ranges, tx_num) = Self::get_block_ranges("tx", &config.transactions_path).await?;
        log::info!("collecting block range info for parquet (log) files...");
        let (log_ranges, log_num) = Self::get_block_ranges("log", &config.logs_path).await?;

        let parquet_block_number = cmp::min(blk_num, cmp::min(tx_num, log_num));

        Ok(ParquetState {
            parquet_block_number,
            block_ranges,
            tx_ranges,
            log_ranges,
        })
    }

    pub async fn status(&self) -> Result<Status> {
        let parquet_block_number = { self.parquet_state.read().await.parquet_block_number };

        let db_max_block_number = self
            .db
            .get_max_block_number()
            .await
            .map_err(Error::GetMaxBlockNumber)?
            .unwrap_or(0);

        let db_min_block_number = self
            .db
            .get_min_block_number()
            .await
            .map_err(Error::GetMinBlockNumber)?
            .unwrap_or(0);

        let db_min_block_number = u32::try_from(db_min_block_number).unwrap();
        let db_max_block_number = u32::try_from(db_max_block_number).unwrap();

        let archive_height = if parquet_block_number > db_min_block_number {
            db_max_block_number
        } else {
            parquet_block_number
        };

        Ok(Status {
            db_max_block_number,
            db_min_block_number,
            parquet_block_number,
            archive_height,
        })
    }

    async fn get_block_ranges(prefix: &str, path: &str) -> Result<(RangeMap, u32)> {
        let mut dir = fs::read_dir(path).await.map_err(Error::ReadParquetDir)?;

        let mut ranges = Vec::new();
        let mut max_block_num = 0;

        while let Some(entry) = dir.next_entry().await.map_err(Error::ReadParquetDir)? {
            let file_name = entry
                .file_name()
                .into_string()
                .map_err(|_| Error::ReadParquetFileName)?;
            let file_name = match file_name.strip_prefix(prefix) {
                Some(file_name) => file_name,
                None => return Err(Error::InvalidParquetFilename(file_name.to_owned())),
            };
            let file_name = match file_name.strip_suffix(".parquet") {
                Some(file_name) => file_name,
                None => continue,
            };
            let (start, end) = file_name
                .split_once('_')
                .ok_or_else(|| Error::InvalidParquetFilename(file_name.to_owned()))?;

            let (start, end) = {
                let start = start
                    .parse::<u32>()
                    .map_err(|_| Error::InvalidParquetFilename(file_name.to_owned()))?;
                let end = end
                    .parse::<u32>()
                    .map_err(|_| Error::InvalidParquetFilename(file_name.to_owned()))?;

                (start, end)
            };

            ranges.push((start, end));
            max_block_num = cmp::max(max_block_num, end - 1);
        }

        ranges.sort_by_key(|r| r.0);

        let range_map =
            RangeMap::from_sorted(&ranges).map_err(|e| Error::CreateRangeMap(Box::new(e)))?;

        Ok((range_map, max_block_num))
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

        let status = self.status().await?;

        let archive_height = status.archive_height;

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

        let serialize_thread = tokio::task::spawn_blocking(move || {
            let mut metrics = QueryMetrics::default();

            let mut bytes = br#"{"data":["#.to_vec();

            let mut comma = false;

            let mut next_block = query.from_block;

            while let Some((res, end)) = rx.blocking_recv() {
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

            let status = serde_json::to_string(&status).unwrap();
            let metrics = serde_json::to_string(&metrics).unwrap();

            bytes.extend_from_slice(
                format!(
                    r#"],"metrics":{},"status":{},"nextBlock":{},"totalTime":{}}}"#,
                    metrics,
                    status,
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

    fn setup_log_pruned_frame(
        &self,
        field_selection: FieldSelection,
        from_block: u32,
        to_block: u32,
    ) -> Result<LazyFrame> {
        let range = (from_block, to_block);

        let parquet_state = self.parquet_state.blocking_read();

        let blocks = self.get_lazy_frame_from_parquet(
            &parquet_state.block_ranges,
            range,
            "block",
            &self.config.blocks_path,
        )?;
        let transactions = self.get_lazy_frame_from_parquet(
            &parquet_state.tx_ranges,
            range,
            "tx",
            &self.config.transactions_path,
        )?;
        let logs = self.get_lazy_frame_from_parquet(
            &parquet_state.log_ranges,
            range,
            "log",
            &self.config.logs_path,
        )?;

        let blocks = blocks.select(field_selection.block.unwrap().to_cols());
        let transactions = transactions.select(field_selection.transaction.unwrap().to_cols());
        let logs = logs.select(field_selection.log.unwrap().to_cols());

        let frame = logs
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

        Ok(frame)
    }

    fn setup_tx_pruned_frame(
        &self,
        field_selection: FieldSelection,
        from_block: u32,
        to_block: u32,
    ) -> Result<LazyFrame> {
        let range = (from_block, to_block);

        let parquet_state = self.parquet_state.blocking_read();

        let blocks = self.get_lazy_frame_from_parquet(
            &parquet_state.block_ranges,
            range,
            "block",
            &self.config.blocks_path,
        )?;
        let transactions = self.get_lazy_frame_from_parquet(
            &parquet_state.tx_ranges,
            range,
            "tx",
            &self.config.transactions_path,
        )?;

        let blocks = blocks.select(field_selection.block.unwrap().to_cols());
        let transactions = transactions.select(field_selection.transaction.unwrap().to_cols());

        let frame = transactions.join(
            blocks,
            &[col("tx_block_number")],
            &[col("block_number")],
            JoinType::Inner,
        );

        Ok(frame)
    }

    fn get_lazy_frame_from_parquet(
        &self,
        map: &RangeMap,
        range: (u32, u32),
        table_name: &'static str,
        folder_path: &str,
    ) -> Result<LazyFrame> {
        let block_ranges = map.get(range.0..range.1).collect::<Vec<_>>();
        if block_ranges.is_empty() {
            return Err(Error::RangeNotFoundInParquetFiles(range, table_name));
        }
        let file_range = block_ranges[0].clone();
        let file_path = format!(
            "{}/{}{}_{}.parquet",
            folder_path, table_name, file_range.start, file_range.end
        );

        let mut main_frame =
            LazyFrame::scan_parquet(file_path, Default::default()).map_err(Error::ScanParquet)?;

        for file_range in block_ranges.iter().skip(1) {
            let file_path = format!(
                "{}/{}{}_{}.parquet",
                folder_path, table_name, file_range.start, file_range.end
            );
            let data_frame = LazyFrame::scan_parquet(file_path, Default::default())
                .map_err(Error::ScanParquet)?;

            main_frame =
                concat(&[main_frame, data_frame], true, true).map_err(Error::UnionFrames)?;
        }

        Ok(main_frame)
    }

    fn query_parquet(&self, query: MiniQuery) -> Result<QueryResult> {
        let mut metrics = QueryMetrics::default();
        let mut data = vec![];

        if !query.logs.is_empty() {
            let logs = self.query_logs(&query)?;
            metrics += logs.metrics;
            for row in logs.data {
                data.push(row);
            }
        }

        if !query.transactions.is_empty() {
            let transactions = self.query_transactions(&query)?;
            metrics += transactions.metrics;
            for row in transactions.data {
                data.push(row);
            }
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
