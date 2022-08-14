use crate::config::DataConfig;
use crate::types::{QueryLogs, QueryResult, Status};
use crate::{Error, Result};
use datafusion::execution::context::{SessionConfig, SessionContext};
use eth_archive_core::db::DbHandle;
use std::cmp;
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct DataCtx {
    db: Arc<DbHandle>,
    session: Arc<RwLock<(SessionContext, u64)>>,
    config: DataConfig,
}

impl DataCtx {
    pub async fn new(db: Arc<DbHandle>, config: DataConfig) -> Result<Self> {
        let session = Self::setup_session(&config).await?;

        let parquet_block_number = Self::get_parquet_block_number(&session).await?.unwrap_or(0);

        let session = Arc::new(RwLock::new((session, parquet_block_number)));

        Ok(Self {
            db,
            session,
            config,
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

    async fn get_parquet_block_number(session: &SessionContext) -> Result<Option<u64>> {
        let get_num = |sql| async {
            let data_frame = session
                .sql(
                    "
                    SELECT
                        MAX(number) as block_number
                    FROM block;
                ",
                )
                .await
                .map_err(Error::BuildQuery)?;

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
        };

        let blk_num = get_num(
            "
                SELECT
                    MAX(number) as block_number
                FROM block;
            ",
        )
        .await?;

        let tx_num = get_num(
            "
                SELECT
                    MAX(block_number) as block_number
                FROM tx;
            ",
        )
        .await?;

        let log_num = get_num(
            "
                SELECT
                    MAX(block_number) as block_number
                FROM log;
            ",
        )
        .await?;

        let parquet_block_number = cmp::min(blk_num, cmp::min(tx_num, log_num));

        Ok(parquet_block_number)
    }

    pub async fn query(&self, query: QueryLogs) -> Result<QueryResult> {
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
                query.addresses[0].to_expr()?.and(
                    col("log.block_number")
                        .gt_eq(lit(query.from_block))
                        .and(col("log.block_number").lt(lit(query.to_block))),
                ),
            )
            .map_err(Error::ApplyBlockRangeFilter)?;

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
                table_partition_cols: Vec::new(),
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
                table_partition_cols: Vec::new(),
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
                table_partition_cols: Vec::new(),
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
