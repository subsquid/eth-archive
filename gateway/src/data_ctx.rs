use crate::config::DataConfig;
use crate::range_map::RangeMap;
use crate::types::{QueryLogs, Status};
use crate::{Error, Result};
use arrow::record_batch::RecordBatch;
use datafusion::execution::context::{SessionConfig, SessionContext};
use eth_archive_core::db::DbHandle;
use eth_archive_core::types::{
    QueryMetrics, QueryResult, ResponseBlock, ResponseLog, ResponseRow, ResponseTransaction,
};
use std::cmp;
use std::sync::Arc;
use std::time::Instant;
use tokio::fs;
use tokio::sync::RwLock;

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

        Ok(Status {
            db_max_block_number,
            db_min_block_number,
            parquet_block_number: self.parquet_state.read().await.parquet_block_number,
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
                .map_err(|_| Error::InvalidParquetSubdirectory)?;
            let file_name = match file_name.strip_prefix(prefix) {
                Some(file_name) => file_name,
                None => return Err(Error::InvalidParquetSubdirectory),
            };
            let file_name = match file_name.strip_suffix(".parquet") {
                Some(file_name) => file_name,
                None => continue,
            };

            let (start, end) = file_name
                .split_once('_')
                .ok_or(Error::InvalidParquetSubdirectory)?;

            let (start, end) = {
                let start = start
                    .parse::<u32>()
                    .map_err(|_| Error::InvalidParquetSubdirectory)?;
                let end = end
                    .parse::<u32>()
                    .map_err(|_| Error::InvalidParquetSubdirectory)?;

                (start, end)
            };

            ranges.push((start, end));
            max_block_num = cmp::max(max_block_num, end - 1);
        }

        ranges.sort_by_key(|r| r.0);

        let range_map =
            RangeMap::from_sorted(&ranges).map_err(|_| Error::InvalidParquetSubdirectory)?;

        Ok((range_map, max_block_num))
    }

    pub async fn query(&self, query: QueryLogs) -> Result<QueryResult> {
        if query.to_block == 0 || query.from_block >= query.to_block {
            return Err(Error::InvalidBlockRange);
        }

        let block_range = query.to_block - query.from_block;

        if block_range > self.config.max_block_range {
            return Err(Error::MaximumBlockRange {
                range: block_range,
                max: self.config.max_block_range,
            });
        }

        let parquet_block_number = self.parquet_state.read().await.parquet_block_number;

        if parquet_block_number >= query.to_block {
            self.query_parquet(query).await
        } else {
            let start_time = Instant::now();

            let query = query.to_sql()?;

            let build_query = start_time.elapsed().as_millis();

            self.db
                .raw_query(build_query, &query)
                .await
                .map_err(Error::SqlQuery)
        }
    }

    async fn setup_pruned_session(&self, from_block: u32, to_block: u32) -> Result<SessionContext> {
        let range = (from_block, to_block);

        let cfg = SessionConfig::new()
            .with_target_partitions(self.config.target_partitions)
            .with_batch_size(self.config.batch_size)
            .with_parquet_pruning(true);

        let mut ctx = SessionContext::with_config(cfg);

        let parquet_state = self.parquet_state.read().await;

        self.register_table(
            &mut ctx,
            &parquet_state.block_ranges,
            range,
            "block",
            &self.config.blocks_path,
        )
        .await?;
        self.register_table(
            &mut ctx,
            &parquet_state.tx_ranges,
            range,
            "tx",
            &self.config.transactions_path,
        )
        .await?;
        self.register_table(
            &mut ctx,
            &parquet_state.log_ranges,
            range,
            "log",
            &self.config.logs_path,
        )
        .await?;

        Ok(ctx)
    }

    async fn register_table(
        &self,
        ctx: &mut SessionContext,
        map: &RangeMap,
        range: (u32, u32),
        table_name: &'static str,
        folder_path: &str,
    ) -> Result<()> {
        use datafusion::prelude::ParquetReadOptions;

        let block_ranges = map.get(range.0..range.1).collect::<Vec<_>>();
        if block_ranges.is_empty() {
            return Err(Error::RangeNotFoundInParquetFiles(range, table_name));
        }
        let file_range = block_ranges[0].clone();
        let file_path = format!(
            "{}/{}{}_{}.parquet",
            folder_path, table_name, file_range.start, file_range.end
        );
        let mut main_frame = ctx
            .read_parquet(
                &file_path,
                ParquetReadOptions {
                    file_extension: ".parquet",
                    table_partition_cols: Vec::new(),
                    parquet_pruning: true,
                    skip_metadata: true,
                },
            )
            .await
            .map_err(Error::RegisterParquet)?;
        for file_range in block_ranges.iter().skip(1) {
            let file_path = format!(
                "{}/{}{}_{}.parquet",
                folder_path, table_name, file_range.start, file_range.end
            );
            let data_frame = ctx
                .read_parquet(
                    &file_path,
                    ParquetReadOptions {
                        file_extension: ".parquet",
                        table_partition_cols: Vec::new(),
                        parquet_pruning: true,
                        skip_metadata: true,
                    },
                )
                .await
                .map_err(Error::RegisterParquet)?;

            main_frame = main_frame
                .union(data_frame)
                .map_err(Error::RegisterParquet)?;
        }
        ctx.register_table(table_name, main_frame)
            .map_err(Error::RegisterParquet)?;

        Ok(())
    }

    async fn query_parquet(&self, query: QueryLogs) -> Result<QueryResult> {
        use datafusion::prelude::*;

        let start_time = Instant::now();

        let select_columns = query.field_selection.to_cols();
        if select_columns.is_empty() {
            return Err(Error::NoFieldsSelected);
        }

        let session = self
            .setup_pruned_session(query.from_block, query.to_block)
            .await?;

        let mut data_frame = session.table("log").map_err(Error::BuildQuery)?;

        data_frame = data_frame
            .join(
                session.table("block").map_err(Error::BuildQuery)?,
                JoinType::Inner,
                &["log.block_number"],
                &["block.number"],
                None,
            )
            .map_err(Error::BuildQuery)?
            .join(
                session.table("tx").map_err(Error::BuildQuery)?,
                JoinType::Inner,
                &["log.block_number", "log.transaction_index"],
                &["tx.block_number", "tx.transaction_index"],
                None,
            )
            .map_err(Error::BuildQuery)?
            .select(select_columns)
            .map_err(Error::BuildQuery)?;

        data_frame = data_frame
            .filter(
                col("log.block_number")
                    .gt_eq(lit(query.from_block))
                    .and(col("log.block_number").lt(lit(query.to_block))),
            )
            .map_err(Error::ApplyBlockRangeFilter)?;

        if !query.addresses.is_empty() {
            let mut addresses = query.addresses;

            let mut expr: Expr = addresses.pop().unwrap().to_expr()?;

            for addr in addresses {
                expr = expr.or(addr.to_expr()?);
            }

            data_frame = data_frame.filter(expr).map_err(Error::ApplyAddrFilters)?;
        }

        let build_query = start_time.elapsed().as_millis();

        let start_time = Instant::now();

        let batches = data_frame.collect().await.map_err(Error::ExecuteQuery)?;

        let run_query = start_time.elapsed().as_millis();

        let start_time = Instant::now();

        let schema = match batches.get(0) {
            Some(batch) => batch.schema(),
            None => {
                return Ok(QueryResult {
                    data: Vec::new(),
                    metrics: QueryMetrics {
                        build_query,
                        run_query,
                        serialize_result: 0,
                        total: build_query + run_query,
                    },
                })
            }
        };

        let batch = RecordBatch::concat(&schema, &batches).map_err(Error::ConcatRecordBatches)?;

        let data = response_rows_from_batch(batch);

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

// defines columns using the record batch and a list of column names
macro_rules! define_cols {
    ($batch:expr, $($name:ident),*) => {
        let schema = $batch.schema();

        $(
            let $name = schema.column_with_name(stringify!($name)).map(|(i, _)| {
                $batch.column(i)
            });
        )*
    };
}

fn response_rows_from_batch(batch: RecordBatch) -> Vec<ResponseRow> {
    use arrow::array::{BinaryArray, BooleanArray, Int64Array, UInt64Array};
    use eth_archive_core::deserialize::{Address, BigInt, BloomFilterBytes, Bytes, Bytes32, Nonce};

    define_cols!(
        batch,
        block_number,
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

    (0..batch.num_rows())
        .map(|i| ResponseRow {
            block: ResponseBlock {
                number: block_number
                    .map(|arr| arr.as_any().downcast_ref::<Int64Array>().unwrap().value(i))
                    .map(BigInt),
                hash: block_hash
                    .map(|arr| arr.as_any().downcast_ref::<BinaryArray>().unwrap().value(i))
                    .map(Bytes32::new),
                parent_hash: block_parent_hash
                    .map(|arr| arr.as_any().downcast_ref::<BinaryArray>().unwrap().value(i))
                    .map(Bytes32::new),
                nonce: block_nonce
                    .map(|arr| arr.as_any().downcast_ref::<UInt64Array>().unwrap().value(i))
                    .map(Nonce),
                sha3_uncles: block_sha3_uncles
                    .map(|arr| arr.as_any().downcast_ref::<BinaryArray>().unwrap().value(i))
                    .map(Bytes32::new),
                logs_bloom: block_logs_bloom
                    .map(|arr| arr.as_any().downcast_ref::<BinaryArray>().unwrap().value(i))
                    .map(BloomFilterBytes::new),
                transactions_root: block_transactions_root
                    .map(|arr| arr.as_any().downcast_ref::<BinaryArray>().unwrap().value(i))
                    .map(Bytes32::new),
                state_root: block_state_root
                    .map(|arr| arr.as_any().downcast_ref::<BinaryArray>().unwrap().value(i))
                    .map(Bytes32::new),
                receipts_root: block_receipts_root
                    .map(|arr| arr.as_any().downcast_ref::<BinaryArray>().unwrap().value(i))
                    .map(Bytes32::new),
                miner: block_miner
                    .map(|arr| arr.as_any().downcast_ref::<BinaryArray>().unwrap().value(i))
                    .map(Address::new),
                difficulty: block_difficulty
                    .map(|arr| arr.as_any().downcast_ref::<BinaryArray>().unwrap().value(i))
                    .map(Bytes::new),
                total_difficulty: block_total_difficulty
                    .map(|arr| arr.as_any().downcast_ref::<BinaryArray>().unwrap().value(i))
                    .map(Bytes::new),
                extra_data: block_extra_data
                    .map(|arr| arr.as_any().downcast_ref::<BinaryArray>().unwrap().value(i))
                    .map(Bytes::new),
                size: block_size
                    .map(|arr| arr.as_any().downcast_ref::<Int64Array>().unwrap().value(i))
                    .map(BigInt),
                gas_limit: block_gas_limit
                    .map(|arr| arr.as_any().downcast_ref::<BinaryArray>().unwrap().value(i))
                    .map(Bytes::new),
                gas_used: block_gas_used
                    .map(|arr| arr.as_any().downcast_ref::<BinaryArray>().unwrap().value(i))
                    .map(Bytes::new),
                timestamp: block_timestamp
                    .map(|arr| arr.as_any().downcast_ref::<Int64Array>().unwrap().value(i))
                    .map(BigInt),
            },
            transaction: ResponseTransaction {
                block_hash: tx_block_hash
                    .map(|arr| arr.as_any().downcast_ref::<BinaryArray>().unwrap().value(i))
                    .map(Bytes32::new),
                block_number: tx_block_number
                    .map(|arr| arr.as_any().downcast_ref::<Int64Array>().unwrap().value(i))
                    .map(BigInt),
                source: tx_source
                    .map(|arr| arr.as_any().downcast_ref::<BinaryArray>().unwrap().value(i))
                    .map(Address::new),
                gas: tx_gas
                    .map(|arr| arr.as_any().downcast_ref::<Int64Array>().unwrap().value(i))
                    .map(BigInt),
                gas_price: tx_gas_price
                    .map(|arr| arr.as_any().downcast_ref::<Int64Array>().unwrap().value(i))
                    .map(BigInt),
                hash: tx_hash
                    .map(|arr| arr.as_any().downcast_ref::<BinaryArray>().unwrap().value(i))
                    .map(Bytes32::new),
                input: tx_input
                    .map(|arr| arr.as_any().downcast_ref::<BinaryArray>().unwrap().value(i))
                    .map(Bytes::new),
                nonce: tx_nonce
                    .map(|arr| arr.as_any().downcast_ref::<UInt64Array>().unwrap().value(i))
                    .map(Nonce),
                dest: match tx_dest
                    .map(|arr| arr.as_any().downcast_ref::<BinaryArray>().unwrap().value(i))
                {
                    Some(addr) if !addr.is_empty() => Some(Address::new(addr)),
                    _ => None,
                },
                transaction_index: tx_transaction_index
                    .map(|arr| arr.as_any().downcast_ref::<Int64Array>().unwrap().value(i))
                    .map(BigInt),
                value: tx_value
                    .map(|arr| arr.as_any().downcast_ref::<BinaryArray>().unwrap().value(i))
                    .map(Bytes::new),
                kind: tx_kind
                    .map(|arr| arr.as_any().downcast_ref::<Int64Array>().unwrap().value(i))
                    .map(BigInt),
                chain_id: tx_chain_id
                    .map(|arr| arr.as_any().downcast_ref::<Int64Array>().unwrap().value(i))
                    .map(BigInt),
                v: tx_v
                    .map(|arr| arr.as_any().downcast_ref::<Int64Array>().unwrap().value(i))
                    .map(BigInt),
                r: tx_r
                    .map(|arr| arr.as_any().downcast_ref::<BinaryArray>().unwrap().value(i))
                    .map(Bytes::new),
                s: tx_s
                    .map(|arr| arr.as_any().downcast_ref::<BinaryArray>().unwrap().value(i))
                    .map(Bytes::new),
            },
            log: ResponseLog {
                address: log_address
                    .map(|arr| arr.as_any().downcast_ref::<BinaryArray>().unwrap().value(i))
                    .map(Address::new),
                block_hash: log_block_hash
                    .map(|arr| arr.as_any().downcast_ref::<BinaryArray>().unwrap().value(i))
                    .map(Bytes32::new),
                block_number: log_block_number
                    .map(|arr| arr.as_any().downcast_ref::<Int64Array>().unwrap().value(i))
                    .map(BigInt),
                data: log_data
                    .map(|arr| arr.as_any().downcast_ref::<BinaryArray>().unwrap().value(i))
                    .map(Bytes::new),
                log_index: log_log_index
                    .map(|arr| arr.as_any().downcast_ref::<Int64Array>().unwrap().value(i))
                    .map(BigInt),
                removed: log_removed.map(|arr| {
                    arr.as_any()
                        .downcast_ref::<BooleanArray>()
                        .unwrap()
                        .value(i)
                }),
                topics: {
                    let mut topics = vec![];

                    if let Some(topic) = log_topic0
                        .map(|arr| arr.as_any().downcast_ref::<BinaryArray>().unwrap().value(i))
                    {
                        if !topic.is_empty() {
                            topics.push(Bytes32::new(topic));
                        }
                    }

                    if let Some(topic) = log_topic1
                        .map(|arr| arr.as_any().downcast_ref::<BinaryArray>().unwrap().value(i))
                    {
                        if !topic.is_empty() {
                            topics.push(Bytes32::new(topic));
                        }
                    }

                    if let Some(topic) = log_topic2
                        .map(|arr| arr.as_any().downcast_ref::<BinaryArray>().unwrap().value(i))
                    {
                        if !topic.is_empty() {
                            topics.push(Bytes32::new(topic));
                        }
                    }

                    if let Some(topic) = log_topic3
                        .map(|arr| arr.as_any().downcast_ref::<BinaryArray>().unwrap().value(i))
                    {
                        if !topic.is_empty() {
                            topics.push(Bytes32::new(topic));
                        }
                    }

                    Some(topics)
                },
                transaction_hash: log_transaction_hash
                    .map(|arr| arr.as_any().downcast_ref::<BinaryArray>().unwrap().value(i))
                    .map(Bytes32::new),
                transaction_index: log_transaction_index
                    .map(|arr| arr.as_any().downcast_ref::<Int64Array>().unwrap().value(i))
                    .map(BigInt),
            },
        })
        .collect()
}
