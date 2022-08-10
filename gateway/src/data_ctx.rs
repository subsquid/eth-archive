use crate::config::DatafusionConfig;
use crate::types::{QueryLogs, QueryResult, Status};
use crate::{Error, Result};
use datafusion::execution::context::{SessionConfig, SessionContext};
use eth_archive_core::db::DbHandle;
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct DataCtx {
    db: Arc<DbHandle>,
    session: Arc<RwLock<SessionContext>>,
    config: DatafusionConfig,
}

impl DataCtx {
    pub async fn new(db: Arc<DbHandle>, config: DatafusionConfig) -> Result<Self> {
        let session = Self::setup_session(&config).await?;
        let session = Arc::new(RwLock::new(session));

        Ok(Self {
            db,
            session,
            config,
        })
    }

    pub async fn status(&self) -> Result<Status> {
        let session = self.session.read().await;

        let db_block_number = self
            .db
            .get_max_block_number()
            .await
            .map_err(Error::GetMaxBlockNumber)?;

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

        Ok(Status {
            db_block_number,
            parquet_block_number,
        })
    }

    pub async fn query(&self, query: QueryLogs) -> Result<QueryResult> {
        todo!()
    }

    async fn setup_session(config: &DatafusionConfig) -> Result<SessionContext> {
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
