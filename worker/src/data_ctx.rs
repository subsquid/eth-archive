use crate::config::Config;
use crate::db::DbHandle;
use crate::db_writer::DbWriter;
use crate::field_selection::FieldSelection;
use crate::serialize_task::SerializeTask;
use crate::types::{MiniLogSelection, MiniQuery, MiniTransactionSelection, Query};
use crate::{Error, Result};
use arrayvec::ArrayVec;
use eth_archive_core::dir_name::DirName;
use eth_archive_core::eth_client::EthClient;
use eth_archive_core::ingest_metrics::IngestMetrics;
use eth_archive_core::rayon_async;
use eth_archive_core::retry::Retry;
use eth_archive_core::s3_client::{Direction, S3Client};
use eth_archive_core::types::{
    BlockRange, QueryResult, ResponseBlock, ResponseLog, ResponseRow, ResponseTransaction,
};
use futures::pin_mut;
use futures::stream::StreamExt;
use polars::prelude::*;
use std::collections::VecDeque;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::{cmp, io};

pub struct DataCtx {
    config: Config,
    db: Arc<DbHandle>,
}

impl DataCtx {
    pub async fn new(config: Config, ingest_metrics: Arc<IngestMetrics>) -> Result<Self> {
        let db = DbHandle::new(&config.db_path, ingest_metrics.clone()).await?;
        let db = Arc::new(db);

        let db_writer = DbWriter::new(db.clone(), &config.data_path);
        let db_writer = Arc::new(db_writer);

        let retry = Retry::new(config.retry);
        let eth_client = EthClient::new(config.ingest.clone(), retry, ingest_metrics.clone())
            .map_err(Error::CreateEthClient)?;
        let eth_client = Arc::new(eth_client);

        // this task is responsible for downloading tip data from rpc node
        tokio::spawn({
            let db_writer = db_writer.clone();
            let db_height = db.db_height();

            async move {
                let best_block = eth_client.clone().get_best_block().await.unwrap();
                let mut start = match config.initial_hot_block_range {
                    Some(range) if best_block > range => best_block - range,
                    _ => 0,
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

        if let Some(data_path) = &config.data_path {
            // this task checks and registers new parquet files to database
            tokio::spawn({
                let start = db.parquet_height();
                let data_path = data_path.clone();
                let db_writer = db_writer.clone();

                async move {
                    if let Err(e) = tokio::fs::create_dir_all(&data_path).await {
                        eprintln!(
                            "failed to create missing data directory:\n{e}\nstopping parquet watcher",
                        );
                        return;
                    }

                    let mut next_start = start;
                    loop {
                        let dir_names = DirName::find_sorted(&data_path, next_start).await.unwrap();

                        for dir_name in dir_names {
                            if !Self::parquet_folder_is_valid(&data_path, dir_name)
                                .await
                                .unwrap()
                            {
                                break;
                            }

                            db_writer.register_parquet_folder(dir_name).await;
                            next_start = dir_name.range.to;
                        }

                        tokio::time::sleep(Duration::from_secs(5)).await;
                    }
                }
            });
        }

        // this task runs periodical compactions on the database
        tokio::spawn(async move {
            loop {
                db_writer.run_compaction().await;
                tokio::time::sleep(Duration::from_secs(config.db_compaction_interval)).await;
            }
        });

        if let Some(data_path) = &config.data_path {
            if let Some(s3_config) = config.s3.into_parsed() {
                let s3_client = S3Client::new(retry, &s3_config)
                    .await
                    .map_err(Error::BuildS3Client)?;
                let s3_client = Arc::new(s3_client);

                s3_client.spawn_s3_sync(Direction::Down, data_path);
            } else {
                log::info!("no s3 config, disabling s3 sync");
            }
        }

        Ok(Self { config, db })
    }

    async fn parquet_folder_is_valid(data_path: &Path, dir_name: DirName) -> Result<bool> {
        let mut path = data_path.to_owned();
        path.push(dir_name.to_string());

        for name in ["block", "tx", "log"] {
            let mut path = path.clone();
            path.push(format!("{name}.parquet"));
            match tokio::fs::File::open(&path).await {
                Err(e) if e.kind() == io::ErrorKind::NotFound => {
                    return Ok(false);
                }
                Err(e) => return Err(Error::ReadParquetDir(e)),
                Ok(_) => (),
            }
        }

        Ok(true)
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
            inclusive_height,
        );

        let query_start = Instant::now();

        let query_task = rayon_async::spawn(move || {
            if query.from_block >= height {
                return Ok(serialize_task);
            }

            let parquet_height = self.db.parquet_height();

            let field_selection = query.field_selection();

            if query.from_block < parquet_height {
                let concurrency = self.config.query_concurrency.get();

                let mut jobs: VecDeque<crossbeam_channel::Receiver<_>> =
                    VecDeque::with_capacity(concurrency);

                for res in self.db.iter_parquet_idxs(query.from_block, to_block)? {
                    if query_start.elapsed().as_millis() >= self.config.resp_time_limit {
                        return Ok(serialize_task);
                    }

                    let (dir_name, parquet_idx) = res?;

                    let logs = query
                        .logs
                        .iter()
                        .filter_map(|log_selection| {
                            let address = match &log_selection.address {
                                Some(address) if !address.is_empty() => address,
                                _ => {
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
                                Some(address) if !address.is_empty() => address,
                                _ => {
                                    return Some(MiniTransactionSelection {
                                        address: tx_selection.address.clone(),
                                        sighash: tx_selection.sighash.clone(),
                                        status: tx_selection.status,
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
                                    status: tx_selection.status,
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
                        include_all_blocks: query.include_all_blocks,
                    };

                    let block_range = BlockRange {
                        from: from_block,
                        to: to_block,
                    };

                    if jobs.len() == concurrency {
                        let job = jobs.pop_front().unwrap();

                        let (res, block_range) = job.recv().unwrap();

                        let res = res?;

                        if !serialize_task.send((res, block_range)) {
                            return Ok(serialize_task);
                        }
                    }

                    let (tx, rx) = crossbeam_channel::bounded(1);

                    rayon::spawn({
                        let ctx = self.clone();
                        move || {
                            tx.send((ctx.query_parquet(dir_name, mini_query), block_range))
                                .ok();
                        }
                    });

                    jobs.push_back(rx);
                }

                while let Some(job) = jobs.pop_front() {
                    let (res, block_range) = job.recv().unwrap();

                    let res = res?;

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
                    if query_start.elapsed().as_millis() >= self.config.resp_time_limit {
                        return Ok(serialize_task);
                    }

                    let end = cmp::min(to_block, start + self.config.db_query_batch_size);

                    let mini_query = MiniQuery {
                        from_block: start,
                        to_block: end,
                        logs: query.log_selection(),
                        transactions: query.tx_selection(),
                        field_selection,
                        include_all_blocks: query.include_all_blocks,
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
        let mut data = Vec::new();

        if !query.logs.is_empty() {
            let logs = self.query_logs(
                &query,
                self.open_log_lazy_frame(dir_name, query.field_selection)?,
            )?;
            data.extend_from_slice(&logs.data);
        }

        if !query.transactions.is_empty() {
            let transactions = self.query_transactions(
                &query,
                self.open_tx_lazy_frame(dir_name, query.field_selection)?,
            )?;
            data.extend_from_slice(&transactions.data);
        }

        if query.include_all_blocks {
            let blocks = self
                .open_all_blocks_lazy_frame(dir_name, &query)?
                .collect()
                .map_err(Error::ExecuteQuery)?;

            append_blocks_to_response_rows(&mut data, blocks)?;
        }

        Ok(QueryResult { data })
    }

    fn query_logs(&self, query: &MiniQuery, mut lazy_frame: LazyFrame) -> Result<QueryResult> {
        use polars::prelude::*;

        lazy_frame = lazy_frame.filter(
            col("log_block_number")
                .gt_eq(lit(query.from_block))
                .and(col("log_block_number").lt(lit(query.to_block))),
        );

        if !query.logs.is_empty() {
            let mut expr: Option<Expr> = None;

            for log in &query.logs {
                if let Some(inner_expr) = log.to_expr() {
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

        let result_frame = lazy_frame.collect().map_err(Error::ExecuteQuery)?;

        let data = response_rows_from_result_frame(result_frame)?;

        Ok(QueryResult { data })
    }

    fn query_transactions(
        &self,
        query: &MiniQuery,
        mut lazy_frame: LazyFrame,
    ) -> Result<QueryResult> {
        use polars::prelude::*;

        lazy_frame = lazy_frame.filter(
            col("tx_block_number")
                .gt_eq(lit(query.from_block))
                .and(col("tx_block_number").lt(lit(query.to_block))),
        );

        if !query.transactions.is_empty() {
            let mut expr: Option<Expr> = None;

            for tx in &query.transactions {
                if let Some(inner_expr) = tx.to_expr() {
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

        let result_frame = lazy_frame.collect().map_err(Error::ExecuteQuery)?;

        let data = tx_response_rows_from_result_frame(result_frame)?;

        Ok(QueryResult { data })
    }

    fn open_log_lazy_frame(
        &self,
        dir_name: DirName,
        field_selection: FieldSelection,
    ) -> Result<LazyFrame> {
        let mut path = self.config.data_path.as_ref().unwrap().clone();
        path.push(dir_name.to_string());

        let blocks = {
            let mut path = path.clone();
            path.push("block.parquet");

            LazyFrame::scan_parquet(&path, scan_parquet_args()).map_err(Error::ScanParquet)?
        };

        let transactions = {
            let mut path = path.clone();
            path.push("tx.parquet");

            LazyFrame::scan_parquet(&path, scan_parquet_args()).map_err(Error::ScanParquet)?
        };

        let logs = {
            let mut path = path.clone();
            path.push("log.parquet");

            LazyFrame::scan_parquet(&path, scan_parquet_args()).map_err(Error::ScanParquet)?
        };

        let blocks = blocks.select(field_selection.block.to_cols());
        let transactions = transactions.select(field_selection.transaction.to_cols());
        let logs = logs.select(field_selection.log.to_cols());

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
        let mut path = self.config.data_path.as_ref().unwrap().clone();
        path.push(dir_name.to_string());

        let blocks = {
            let mut path = path.clone();
            path.push("block.parquet");

            LazyFrame::scan_parquet(&path, scan_parquet_args()).map_err(Error::ScanParquet)?
        };

        let transactions = {
            let mut path = path.clone();
            path.push("tx.parquet");

            LazyFrame::scan_parquet(&path, scan_parquet_args()).map_err(Error::ScanParquet)?
        };

        let blocks = blocks.select(field_selection.block.to_cols());
        let mut transaction_cols = field_selection.transaction.to_cols();
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

    fn open_all_blocks_lazy_frame(
        &self,
        dir_name: DirName,
        query: &MiniQuery,
    ) -> Result<LazyFrame> {
        let mut path = self.config.data_path.as_ref().unwrap().clone();
        path.push(dir_name.to_string());

        let mut blocks = {
            let mut path = path.clone();
            path.push("block.parquet");

            LazyFrame::scan_parquet(&path, scan_parquet_args()).map_err(Error::ScanParquet)?
        };

        blocks = blocks.select(query.field_selection.block.to_cols());

        blocks = blocks.filter(
            col("block_number")
                .gt_eq(lit(query.from_block))
                .and(col("block_number").lt(lit(query.to_block))),
        );

        Ok(blocks)
    }
}

// defines columns using the result frame and a list of column names
macro_rules! define_cols {
    ($batch:expr, $schema:expr, $($name:ident, $arrow_type:ident),*) => {
        let columns = $batch.columns();
        $(
            let $name = $schema.get_full(stringify!($name)).map(|(i, _, _)| {
                columns.get(i).unwrap()
            }).map(|arr| {
                arr.as_any()
                    .downcast_ref::<$arrow_type>()
                    .unwrap()
            });
        )*
    };
}

macro_rules! map_from_arrow {
    ($src_field:ident, $map_type:expr, $idx:expr) => {
        $src_field.map(|arr| arr.get($idx).unwrap()).map($map_type)
    };
}

macro_rules! map_from_arrow_opt {
    ($src_field:ident, $map_type:expr, $idx:expr) => {
        match $src_field.map(|arr| arr.get($idx)) {
            Some(Some(val)) => Some($map_type(val)),
            _ => None,
        }
    };
}

fn append_blocks_to_response_rows(
    data: &mut Vec<ResponseRow>,
    result_frame: DataFrame,
) -> Result<()> {
    use eth_archive_core::deserialize::{
        Address, BigUnsigned, BloomFilterBytes, Bytes, Bytes32, Index,
    };
    use polars::export::arrow::array::{self, UInt32Array, UInt64Array};

    type BinaryArray = array::BinaryArray<i64>;

    let schema = result_frame.schema();

    for batch in result_frame.iter_chunks() {
        #[rustfmt::skip]
        define_cols!(
            batch,
            schema,
            block_number, UInt32Array,
            block_parent_hash, BinaryArray,
            block_sha3_uncles, BinaryArray,
            block_miner, BinaryArray,
            block_state_root, BinaryArray,
            block_transactions_root, BinaryArray,
            block_receipts_root, BinaryArray,
            block_logs_bloom, BinaryArray,
            block_difficulty, BinaryArray,
            block_gas_limit, BinaryArray,
            block_gas_used, BinaryArray,
            block_timestamp, BinaryArray,
            block_extra_data, BinaryArray,
            block_mix_hash, BinaryArray,
            block_nonce, UInt64Array,
            block_total_difficulty, BinaryArray,
            block_base_fee_per_gas, BinaryArray,
            block_size, BinaryArray,
            block_hash, BinaryArray
        );

        let len = block_number.as_ref().unwrap().len();
        for i in 0..len {
            let response_row = ResponseRow {
                block: ResponseBlock {
                    parent_hash: map_from_arrow!(block_parent_hash, Bytes32::new, i),
                    sha3_uncles: map_from_arrow!(block_sha3_uncles, Bytes32::new, i),
                    miner: map_from_arrow!(block_miner, Address::new, i),
                    state_root: map_from_arrow!(block_state_root, Bytes32::new, i),
                    transactions_root: map_from_arrow!(block_transactions_root, Bytes32::new, i),
                    receipts_root: map_from_arrow!(block_receipts_root, Bytes32::new, i),
                    logs_bloom: map_from_arrow!(block_logs_bloom, BloomFilterBytes::new, i),
                    difficulty: map_from_arrow_opt!(block_difficulty, Bytes::new, i),
                    number: map_from_arrow!(block_number, Index, i),
                    gas_limit: map_from_arrow!(block_gas_limit, Bytes::new, i),
                    gas_used: map_from_arrow!(block_gas_used, Bytes::new, i),
                    timestamp: map_from_arrow!(block_timestamp, Bytes::new, i),
                    extra_data: map_from_arrow!(block_extra_data, Bytes::new, i),
                    mix_hash: map_from_arrow_opt!(block_mix_hash, Bytes32::new, i),
                    nonce: map_from_arrow_opt!(block_nonce, BigUnsigned, i),
                    total_difficulty: map_from_arrow_opt!(block_total_difficulty, Bytes::new, i),
                    base_fee_per_gas: map_from_arrow_opt!(block_base_fee_per_gas, Bytes::new, i),
                    size: map_from_arrow!(block_size, Bytes::new, i),
                    hash: map_from_arrow_opt!(block_hash, Bytes32::new, i),
                },
                transaction: None,
                log: None,
            };

            data.push(response_row);
        }
    }

    Ok(())
}

fn response_rows_from_result_frame(result_frame: DataFrame) -> Result<Vec<ResponseRow>> {
    use eth_archive_core::deserialize::{
        Address, BigUnsigned, BloomFilterBytes, Bytes, Bytes32, Index,
    };
    use polars::export::arrow::array::{self, BooleanArray, UInt32Array, UInt64Array};

    type BinaryArray = array::BinaryArray<i64>;

    let mut data = Vec::new();

    let schema = result_frame.schema();

    for batch in result_frame.iter_chunks() {
        #[rustfmt::skip]
        define_cols!(
            batch,
            schema,
            block_parent_hash, BinaryArray,
            block_sha3_uncles, BinaryArray,
            block_miner, BinaryArray,
            block_state_root, BinaryArray,
            block_transactions_root, BinaryArray,
            block_receipts_root, BinaryArray,
            block_logs_bloom, BinaryArray,
            block_difficulty, BinaryArray,
            block_gas_limit, BinaryArray,
            block_gas_used, BinaryArray,
            block_timestamp, BinaryArray,
            block_extra_data, BinaryArray,
            block_mix_hash, BinaryArray,
            block_nonce, UInt64Array,
            block_total_difficulty, BinaryArray,
            block_base_fee_per_gas, BinaryArray,
            block_size, BinaryArray,
            block_hash, BinaryArray,
            tx_kind, UInt32Array,
            tx_nonce, UInt64Array,
            tx_dest, BinaryArray,
            tx_gas, BinaryArray,
            tx_value, BinaryArray,
            tx_input, BinaryArray,
            tx_max_priority_fee_per_gas, BinaryArray,
            tx_max_fee_per_gas, BinaryArray,
            tx_y_parity, UInt32Array,
            tx_chain_id, UInt32Array,
            tx_v, UInt64Array,
            tx_r, BinaryArray,
            tx_s, BinaryArray,
            tx_source, BinaryArray,
            tx_block_hash, BinaryArray,
            tx_gas_price, BinaryArray,
            tx_hash, BinaryArray,
            tx_status, UInt32Array,
            log_address, BinaryArray,
            log_block_hash, BinaryArray,
            log_block_number, UInt32Array,
            log_data, BinaryArray,
            log_log_index, UInt32Array,
            log_removed, BooleanArray,
            log_topic0, BinaryArray,
            log_topic1, BinaryArray,
            log_topic2, BinaryArray,
            log_topic3, BinaryArray,
            log_transaction_hash, BinaryArray,
            log_transaction_index, UInt32Array
        );

        let len = log_block_number.as_ref().unwrap().len();

        for i in 0..len {
            let response_row = ResponseRow {
                block: ResponseBlock {
                    parent_hash: map_from_arrow!(block_parent_hash, Bytes32::new, i),
                    sha3_uncles: map_from_arrow!(block_sha3_uncles, Bytes32::new, i),
                    miner: map_from_arrow!(block_miner, Address::new, i),
                    state_root: map_from_arrow!(block_state_root, Bytes32::new, i),
                    transactions_root: map_from_arrow!(block_transactions_root, Bytes32::new, i),
                    receipts_root: map_from_arrow!(block_receipts_root, Bytes32::new, i),
                    logs_bloom: map_from_arrow!(block_logs_bloom, BloomFilterBytes::new, i),
                    difficulty: map_from_arrow_opt!(block_difficulty, Bytes::new, i),
                    number: map_from_arrow!(log_block_number, Index, i),
                    gas_limit: map_from_arrow!(block_gas_limit, Bytes::new, i),
                    gas_used: map_from_arrow!(block_gas_used, Bytes::new, i),
                    timestamp: map_from_arrow!(block_timestamp, Bytes::new, i),
                    extra_data: map_from_arrow!(block_extra_data, Bytes::new, i),
                    mix_hash: map_from_arrow_opt!(block_mix_hash, Bytes32::new, i),
                    nonce: map_from_arrow_opt!(block_nonce, BigUnsigned, i),
                    total_difficulty: map_from_arrow_opt!(block_total_difficulty, Bytes::new, i),
                    base_fee_per_gas: map_from_arrow_opt!(block_base_fee_per_gas, Bytes::new, i),
                    size: map_from_arrow!(block_size, Bytes::new, i),
                    hash: map_from_arrow_opt!(block_hash, Bytes32::new, i),
                },
                transaction: Some(ResponseTransaction {
                    kind: map_from_arrow_opt!(tx_kind, Index, i),
                    nonce: map_from_arrow!(tx_nonce, BigUnsigned, i),
                    dest: map_from_arrow_opt!(tx_dest, Address::new, i),
                    gas: map_from_arrow!(tx_gas, Bytes::new, i),
                    value: map_from_arrow!(tx_value, Bytes::new, i),
                    input: map_from_arrow!(tx_input, Bytes::new, i),
                    max_priority_fee_per_gas: map_from_arrow_opt!(
                        tx_max_priority_fee_per_gas,
                        Bytes::new,
                        i
                    ),
                    max_fee_per_gas: map_from_arrow_opt!(tx_max_fee_per_gas, Bytes::new, i),
                    y_parity: map_from_arrow_opt!(tx_y_parity, Index, i),
                    chain_id: map_from_arrow_opt!(tx_chain_id, Index, i),
                    v: map_from_arrow_opt!(tx_v, BigUnsigned, i),
                    r: map_from_arrow!(tx_r, Bytes::new, i),
                    s: map_from_arrow!(tx_s, Bytes::new, i),
                    source: map_from_arrow_opt!(tx_source, Address::new, i),
                    block_hash: map_from_arrow!(tx_block_hash, Bytes32::new, i),
                    block_number: map_from_arrow!(log_block_number, Index, i),
                    transaction_index: map_from_arrow!(log_transaction_index, Index, i),
                    gas_price: map_from_arrow!(tx_gas_price, Bytes::new, i),
                    hash: map_from_arrow!(tx_hash, Bytes32::new, i),
                    status: map_from_arrow_opt!(tx_status, Index, i),
                }),
                log: Some(ResponseLog {
                    address: map_from_arrow!(log_address, Address::new, i),
                    block_hash: map_from_arrow!(log_block_hash, Bytes32::new, i),
                    block_number: map_from_arrow!(log_block_number, Index, i),
                    data: map_from_arrow!(log_data, Bytes::new, i),
                    log_index: map_from_arrow!(log_log_index, Index, i),
                    removed: log_removed.and_then(|arr| arr.get(i)),
                    topics: {
                        let mut topics = ArrayVec::new();

                        if let Some(Some(topic)) = log_topic0.map(|arr| arr.get(i)) {
                            topics.push(Bytes32::new(topic));
                        }

                        if let Some(Some(topic)) = log_topic1.map(|arr| arr.get(i)) {
                            topics.push(Bytes32::new(topic));
                        }

                        if let Some(Some(topic)) = log_topic2.map(|arr| arr.get(i)) {
                            topics.push(Bytes32::new(topic));
                        }

                        if let Some(Some(topic)) = log_topic3.map(|arr| arr.get(i)) {
                            topics.push(Bytes32::new(topic));
                        }

                        Some(topics)
                    },
                    transaction_hash: map_from_arrow!(log_transaction_hash, Bytes32::new, i),
                    transaction_index: map_from_arrow!(log_transaction_index, Index, i),
                }),
            };

            data.push(response_row);
        }
    }

    Ok(data)
}

fn tx_response_rows_from_result_frame(result_frame: DataFrame) -> Result<Vec<ResponseRow>> {
    use eth_archive_core::deserialize::{
        Address, BigUnsigned, BloomFilterBytes, Bytes, Bytes32, Index,
    };
    use polars::export::arrow::array::{self, UInt32Array, UInt64Array};

    type BinaryArray = array::BinaryArray<i64>;

    let mut data = Vec::new();

    let schema = result_frame.schema();

    for batch in result_frame.iter_chunks() {
        #[rustfmt::skip]
        define_cols!(
            batch,
            schema,
            block_parent_hash, BinaryArray,
            block_sha3_uncles, BinaryArray,
            block_miner, BinaryArray,
            block_state_root, BinaryArray,
            block_transactions_root, BinaryArray,
            block_receipts_root, BinaryArray,
            block_logs_bloom, BinaryArray,
            block_difficulty, BinaryArray,
            block_gas_limit, BinaryArray,
            block_gas_used, BinaryArray,
            block_timestamp, BinaryArray,
            block_extra_data, BinaryArray,
            block_mix_hash, BinaryArray,
            block_nonce, UInt64Array,
            block_total_difficulty, BinaryArray,
            block_base_fee_per_gas, BinaryArray,
            block_size, BinaryArray,
            block_hash, BinaryArray,
            tx_kind, UInt32Array,
            tx_nonce, UInt64Array,
            tx_dest, BinaryArray,
            tx_gas, BinaryArray,
            tx_value, BinaryArray,
            tx_input, BinaryArray,
            tx_max_priority_fee_per_gas, BinaryArray,
            tx_max_fee_per_gas, BinaryArray,
            tx_y_parity, UInt32Array,
            tx_chain_id, UInt32Array,
            tx_v, UInt64Array,
            tx_r, BinaryArray,
            tx_s, BinaryArray,
            tx_source, BinaryArray,
            tx_block_hash, BinaryArray,
            tx_block_number, UInt32Array,
            tx_transaction_index, UInt32Array,
            tx_gas_price, BinaryArray,
            tx_hash, BinaryArray,
            tx_status, UInt32Array
        );

        let len = tx_block_number.as_ref().unwrap().len();

        for i in 0..len {
            let response_row = ResponseRow {
                block: ResponseBlock {
                    parent_hash: map_from_arrow!(block_parent_hash, Bytes32::new, i),
                    sha3_uncles: map_from_arrow!(block_sha3_uncles, Bytes32::new, i),
                    miner: map_from_arrow!(block_miner, Address::new, i),
                    state_root: map_from_arrow!(block_state_root, Bytes32::new, i),
                    transactions_root: map_from_arrow!(block_transactions_root, Bytes32::new, i),
                    receipts_root: map_from_arrow!(block_receipts_root, Bytes32::new, i),
                    logs_bloom: map_from_arrow!(block_logs_bloom, BloomFilterBytes::new, i),
                    difficulty: map_from_arrow_opt!(block_difficulty, Bytes::new, i),
                    number: map_from_arrow!(tx_block_number, Index, i),
                    gas_limit: map_from_arrow!(block_gas_limit, Bytes::new, i),
                    gas_used: map_from_arrow!(block_gas_used, Bytes::new, i),
                    timestamp: map_from_arrow!(block_timestamp, Bytes::new, i),
                    extra_data: map_from_arrow!(block_extra_data, Bytes::new, i),
                    mix_hash: map_from_arrow_opt!(block_mix_hash, Bytes32::new, i),
                    nonce: map_from_arrow_opt!(block_nonce, BigUnsigned, i),
                    total_difficulty: map_from_arrow_opt!(block_total_difficulty, Bytes::new, i),
                    base_fee_per_gas: map_from_arrow_opt!(block_base_fee_per_gas, Bytes::new, i),
                    size: map_from_arrow!(block_size, Bytes::new, i),
                    hash: map_from_arrow_opt!(block_hash, Bytes32::new, i),
                },
                transaction: Some(ResponseTransaction {
                    kind: map_from_arrow_opt!(tx_kind, Index, i),
                    nonce: map_from_arrow!(tx_nonce, BigUnsigned, i),
                    dest: map_from_arrow_opt!(tx_dest, Address::new, i),
                    gas: map_from_arrow!(tx_gas, Bytes::new, i),
                    value: map_from_arrow!(tx_value, Bytes::new, i),
                    input: map_from_arrow!(tx_input, Bytes::new, i),
                    max_priority_fee_per_gas: map_from_arrow_opt!(
                        tx_max_priority_fee_per_gas,
                        Bytes::new,
                        i
                    ),
                    max_fee_per_gas: map_from_arrow_opt!(tx_max_fee_per_gas, Bytes::new, i),
                    y_parity: map_from_arrow_opt!(tx_y_parity, Index, i),
                    chain_id: map_from_arrow_opt!(tx_chain_id, Index, i),
                    v: map_from_arrow_opt!(tx_v, BigUnsigned, i),
                    r: map_from_arrow!(tx_r, Bytes::new, i),
                    s: map_from_arrow!(tx_s, Bytes::new, i),
                    source: map_from_arrow_opt!(tx_source, Address::new, i),
                    block_hash: map_from_arrow!(tx_block_hash, Bytes32::new, i),
                    block_number: map_from_arrow!(tx_block_number, Index, i),
                    transaction_index: map_from_arrow!(tx_transaction_index, Index, i),
                    gas_price: map_from_arrow!(tx_gas_price, Bytes::new, i),
                    hash: map_from_arrow!(tx_hash, Bytes32::new, i),
                    status: map_from_arrow_opt!(tx_status, Index, i),
                }),
                log: None,
            };

            data.push(response_row);
        }
    }

    Ok(data)
}

pub fn scan_parquet_args() -> ScanArgsParquet {
    ScanArgsParquet {
        n_rows: None,
        cache: true,
        parallel: ParallelStrategy::RowGroups,
        rechunk: true,
        row_count: None,
        low_memory: false,
        cloud_options: None,
    }
}
