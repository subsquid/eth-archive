use crate::config::DataConfig;
use crate::types::{QueryLogs, QueryResult, Status};
use crate::{Error, Result};
use datafusion::execution::context::{SessionConfig, SessionContext};
use eth_archive_core::db::DbHandle;
use range_map::{Range, RangeMap as RangeMapImpl};
use std::cmp;
use std::sync::Arc;
use tokio::fs;
use tokio::sync::RwLock;

type RangeMap = RangeMapImpl<u64, u64>;

pub struct DataCtx {
    db: Arc<DbHandle>,
    session: Arc<RwLock<(SessionContext, u64)>>,
    config: DataConfig,
    block_ranges: RangeMap,
    tx_ranges: RangeMap,
    log_ranges: RangeMap,
}

impl DataCtx {
    pub async fn new(db: Arc<DbHandle>, config: DataConfig) -> Result<Self> {
        log::info!("setting up datafusion context...");

        let session = Self::setup_session(&config).await?;

        let parquet_block_number = Self::get_parquet_block_number(&session).await?.unwrap_or(0);

        let session = Arc::new(RwLock::new((session, parquet_block_number)));

        log::info!("collecting block range info for parquet (block header) files...");
        let block_ranges = Self::get_block_ranges(&config.blocks_path).await?;
        log::info!("collecting block range info for parquet (transaction) files...");
        let tx_ranges = Self::get_block_ranges(&config.transactions_path).await?;
        log::info!("collecting block range info for parquet (log) files...");
        let log_ranges = Self::get_block_ranges(&config.logs_path).await?;

        Ok(Self {
            db,
            session,
            config,
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
            parquet_block_number: self.session.read().await.1,
        })
    }

    async fn get_block_ranges(path: &str) -> Result<RangeMap> {
        let mut dir = fs::read_dir(path).await.map_err(Error::ReadParquetDir)?;

        let mut nums = Vec::new();

        while let Some(entry) = dir.next_entry().await.map_err(Error::ReadParquetDir)? {
            let end = entry
                .file_name()
                .into_string()
                .map_err(|_| Error::InvalidParquetSubdirectory)?
                .split_once('=')
                .ok_or(Error::InvalidParquetSubdirectory)?
                .1
                .parse::<u64>()
                .map_err(|_| Error::InvalidParquetSubdirectory)?;

            nums.push(end);
        }

        nums.sort();

        let ranges = nums
            .windows(2)
            .map(|range| {
                let start = range[0];
                let end = range[1];

                (
                    Range {
                        start,
                        end: end - 1,
                    },
                    end,
                )
            })
            .collect();

        let map = RangeMap::from_sorted_vec(ranges);

        Ok(map)
    }

    async fn get_parquet_block_number(session: &SessionContext) -> Result<Option<u64>> {
        let get_num = |sql: &str| {
            let sql = sql.to_owned();

            async move {
                let data_frame = session.sql(&sql).await.map_err(Error::BuildQuery)?;

                let batches = data_frame.collect().await.map_err(Error::ExecuteQuery)?;
                let data = arrow::json::writer::record_batches_to_json_rows(&batches)
                    .map_err(Error::CollectResults)?;

                let parquet_block_number = match data.get(0) {
                    Some(b) => match b.get("block_number") {
                        Some(num) => Some(num.as_u64().ok_or(Error::InvalidBlockNumber)?),
                        None => None,
                    },
                    None => None,
                };

                Ok(parquet_block_number)
            }
        };

        let blk_num = get_num(
            "
                SELECT
                    MAX(block.number) as block_number
                FROM block;
            ",
        )
        .await?;

        let tx_num = get_num(
            "
                SELECT
                    MAX(tx.block_number) as block_number
                FROM tx;
            ",
        )
        .await?;

        let log_num = get_num(
            "
                SELECT
                    MAX(log.block_number) as block_number
                FROM log;
            ",
        )
        .await?;

        let parquet_block_number = cmp::min(blk_num, cmp::min(tx_num, log_num));

        Ok(parquet_block_number)
    }

    pub async fn query(&self, query: QueryLogs) -> Result<QueryResult> {
        if query.to_block == 0 || query.from_block > query.to_block {
            return Err(Error::InvalidBlockRange);
        }

        let block_range = query.to_block - query.from_block;

        if block_range > self.config.max_block_range as u64 {
            return Err(Error::MaximumBlockRange {
                range: block_range,
                max: self.config.max_block_range,
            });
        }

        if self.session.read().await.1 >= query.to_block {
            self.query_parquet(query).await
        } else {
            let data = self
                .db
                .raw_query_to_json(&query.to_sql())
                .await
                .map_err(Error::SqlQuery)?;

            Ok(QueryResult { data })
        }
    }

    pub async fn sql(&self, sql: &str) -> Result<QueryResult> {
        let session = &self.session.read().await.0;

        let data_frame = session.sql(sql).await.map_err(Error::BuildQuery)?;

        let batches = data_frame.collect().await.map_err(Error::ExecuteQuery)?;
        let data = arrow::json::writer::record_batches_to_json_rows(&batches)
            .map_err(Error::CollectResults)?;

        Ok(QueryResult { data })
    }

    async fn query_parquet(&self, query: QueryLogs) -> Result<QueryResult> {
        use datafusion::prelude::*;

        let select_columns = query.field_selection.to_cols();
        if select_columns.is_empty() {
            return Err(Error::NoFieldsSelected);
        }

        let session = &self.session.read().await.0;

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

        data_frame = data_frame
            .filter(
                col("tx.block_number")
                    .gt_eq(lit(query.from_block))
                    .and(col("tx.block_number").lt(lit(query.to_block))),
            )
            .map_err(Error::ApplyBlockRangeFilter)?;

        data_frame = data_frame
            .filter(
                col("block.number")
                    .gt_eq(lit(query.from_block))
                    .and(col("block.number").lt(lit(query.to_block))),
            )
            .map_err(Error::ApplyBlockRangeFilter)?;

        if let Some(end) = self.block_ranges.get(query.to_block - 1) {
            let mut filter_expr = col("block.block_range").eq(lit(end.to_string()));

            if let Some(start) = self.block_ranges.get(query.from_block) {
                filter_expr = filter_expr.or(col("block.block_range").eq(lit(start.to_string())));
            }

            data_frame = data_frame
                .filter(filter_expr)
                .map_err(Error::ApplyBlockRangeFilter)?;
        }

        if let Some(end) = self.tx_ranges.get(query.to_block - 1) {
            let mut filter_expr = col("tx.block_range").eq(lit(end.to_string()));

            if let Some(start) = self.tx_ranges.get(query.from_block) {
                filter_expr = filter_expr.or(col("tx.block_range").eq(lit(start.to_string())));
            }

            data_frame = data_frame
                .filter(filter_expr)
                .map_err(Error::ApplyBlockRangeFilter)?;
        }

        if let Some(end) = self.log_ranges.get(query.to_block - 1) {
            let mut filter_expr = col("log.block_range").eq(lit(end.to_string()));

            if let Some(start) = self.log_ranges.get(query.from_block) {
                filter_expr = filter_expr.or(col("log.block_range").eq(lit(start.to_string())));
            }

            data_frame = data_frame
                .filter(filter_expr)
                .map_err(Error::ApplyBlockRangeFilter)?;
        }

        if !query.addresses.is_empty() {
            let mut addresses = query.addresses;

            let mut expr: Expr = addresses.pop().unwrap().to_expr()?;

            for addr in addresses {
                expr = expr.or(addr.to_expr()?);
            }

            data_frame = data_frame.filter(expr).map_err(Error::ApplyAddrFilters)?;
        }

        let batches = data_frame.collect().await.map_err(Error::ExecuteQuery)?;
        let data = arrow::json::writer::record_batches_to_json_rows(&batches)
            .map_err(Error::CollectResults)?;

        Ok(QueryResult { data })
    }

    async fn setup_session(config: &DataConfig) -> Result<SessionContext> {
        use datafusion::datasource::file_format::parquet::ParquetFormat;
        use datafusion::datasource::listing::ListingOptions;

        let cfg = SessionConfig::new()
            .with_target_partitions(config.target_partitions)
            .with_batch_size(config.batch_size);

        let ctx = SessionContext::with_config(cfg);

        let file_format = ParquetFormat::default().with_enable_pruning(true);
        let file_format = Arc::new(file_format);

        ctx.register_listing_table(
            "block",
            &config.blocks_path,
            ListingOptions {
                file_extension: ".parquet".to_owned(),
                format: file_format.clone(),
                table_partition_cols: vec!["block_range".to_owned()],
                collect_stat: false,
                target_partitions: config.target_partitions,
            },
            None,
        )
        .await
        .map_err(Error::RegisterParquet)?;

        ctx.register_listing_table(
            "tx",
            &config.transactions_path,
            ListingOptions {
                file_extension: ".parquet".to_owned(),
                format: file_format.clone(),
                table_partition_cols: vec!["block_range".to_owned()],
                collect_stat: false,
                target_partitions: config.target_partitions,
            },
            None,
        )
        .await
        .map_err(Error::RegisterParquet)?;

        ctx.register_listing_table(
            "log",
            &config.logs_path,
            ListingOptions {
                file_extension: ".parquet".to_owned(),
                format: file_format.clone(),
                table_partition_cols: vec!["block_range".to_owned()],
                collect_stat: false,
                target_partitions: config.target_partitions,
            },
            None,
        )
        .await
        .map_err(Error::RegisterParquet)?;

        Ok(ctx)
    }
}
