use crate::config::DataConfig;
use crate::types::{QueryLogs, QueryResult, Status};
use crate::{Error, Result};
use datafusion::execution::context::{SessionConfig, SessionContext};
use eth_archive_core::db::DbHandle;
use std::mem;
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct DataCtx {
    db: Arc<DbHandle>,
    session: Arc<RwLock<SessionContext>>,
}

impl DataCtx {
    pub async fn new(db: Arc<DbHandle>, config: DataConfig) -> Result<Self> {
        let session = Self::setup_session(&config).await?;
        let session = Arc::new(RwLock::new(session));

        Ok(Self { db, session })
    }

    pub async fn status(&self) -> Result<Status> {
        let db_block_number = self
            .db
            .get_max_block_number()
            .await
            .map_err(Error::GetMaxBlockNumber)?;

        let session = self.session.read().await;

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

        mem::drop(session);

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
        self.query_parquet(query).await
    }

    async fn query_parquet(&self, query: QueryLogs) -> Result<QueryResult> {
        use datafusion::prelude::*;

        let select_columns = query.field_selection.to_cols();
        if select_columns.is_empty() {
            return Err(Error::NoFieldsSelected);
        }

        let session = self.session.read().await;

        let mut data_frame = session.table("log").map_err(Error::BuildQuery)?;

        if !query.addresses.is_empty() {
            let mut addresses = query.addresses;

            let mut expr: Expr = addresses.pop().unwrap().into();

            for addr in addresses {
                expr = expr.or(addr.into());
            }

            data_frame = data_frame.filter(expr).map_err(Error::ApplyAddrFilters)?;
        }

        data_frame = data_frame
            .join(
                session.table("block").map_err(Error::BuildQuery)?,
                JoinType::Inner,
                &["log.block_number"],
                &["block.number"],
            )
            .map_err(Error::BuildQuery)?
            .join(
                session.table("tx").map_err(Error::BuildQuery)?,
                JoinType::Inner,
                &["log.block_number", "log.transaction_index"],
                &["tx.block_number", "tx.transaction_index"],
            )
            .map_err(Error::BuildQuery)?
            .select(select_columns)
            .map_err(Error::BuildQuery)?;

        data_frame = data_frame
            .filter(Expr::Between {
                expr: Box::new(col("log.block_number")),
                negated: false,
                low: Box::new(lit(query.from_block)),
                high: Box::new(lit(query.to_block)),
            })
            .map_err(Error::ApplyBlockRangeFilter)?;

        mem::drop(session);

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
