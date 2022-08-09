use crate::config::HttpServerConfig;
use crate::data_ctx::DataCtx;
use crate::error::{Error, Result};
use crate::field_selection::LogFieldSelection;
use crate::options::Options;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::ListingOptions;
use datafusion::execution::context::{SessionConfig, SessionContext};
use datafusion::prelude::*;
use eth_archive_core::db::DbHandle;
use serde::Deserialize;
use std::sync::Arc;
use tokio::sync::RwLock;

use serde_json::Value as JsonValue;

use actix_web::{middleware, web, App, HttpServer};

pub struct Server {}

impl Server {
    pub fn run(options: &Options) -> Result<Self> {
        let config =
            tokio::fs::read_to_string(options.cfg_path.as_deref().unwrap_or("EthGateway.toml"))
                .await
                .map_err(Error::ReadConfigFile)?;

        let config: Config = toml::de::from_str(&config).map_err(Error::ParseConfig)?;

        let db = DbHandle::new(false, &config.db)
            .await
            .map_err(|e| Error::CreateDbHandle(Box::new(e)))?;
        let db = Arc::new(db);

        let data_ctx = DataCtx::new(db, config.datafusion).await?;
        let data_ctx = Arc::new(data_ctx);

        HttpServer::new(move || {
            App::new()
                .wrap(middleware::Compress::default())
                .app_data(web::Data::new(data_ctx.clone()))
                .service(web::resource("/query").route(web::post().to(query_logs)))
                .service(web::resource("/status").route(web::get().to(status)))
        })
        .bind((config.http_server.ip, config.http_server.port))
        .map_err(Error::BindHttpServer)?
        .run()
        .await
        .map_err(Error::RunHttpServer)
    }
}

async fn status(ctx: web::Data<Arc<RwLock<SessionContext>>>) -> Result<web::Json<JsonValue>> {
    let ctx = ctx.read().await;

    let data_frame = ctx
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
    let block_number = data
        .get(0)
        .ok_or(Error::NoBlocks)?
        .get("block_number")
        .ok_or(Error::InvalidBlockNumber)?
        .as_u64()
        .ok_or(Error::InvalidBlockNumber)?;

    let data_frame = ctx
        .sql(&format!(
            "
        SELECT number, hash, timestamp from block WHERE number = {}
    ",
            block_number
        ))
        .await
        .map_err(Error::BuildQuery)?;
    let batches = data_frame.collect().await.map_err(Error::ExecuteQuery)?;
    let mut data = arrow::json::writer::record_batches_to_json_rows(&batches)
        .map_err(Error::CollectResults)?;
    Ok(web::Json(JsonValue::Object(
        data.pop().ok_or(Error::NoBlocks)?,
    )))
}

async fn query_logs(
    query: web::Json<QueryLogs>,
    ctx: web::Data<Arc<RwLock<SessionContext>>>,
) -> Result<web::Json<JsonValue>> {
    let ctx = ctx.read().await;

    let query = query.into_inner();
    let select_columns = query.field_selection.to_cols();
    if select_columns.is_empty() {
        return Err(Error::NoFieldsSelected);
    }
    let mut data_frame = ctx.table("log").map_err(Error::BuildQuery)?;

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
            ctx.table("block").map_err(Error::BuildQuery)?,
            JoinType::Inner,
            &["log.block_number"],
            &["block.number"],
        )
        .map_err(Error::BuildQuery)?
        .join(
            ctx.table("tx").map_err(Error::BuildQuery)?,
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

    let batches = data_frame.collect().await.map_err(Error::ExecuteQuery)?;
    let data = arrow::json::writer::record_batches_to_json_rows(&batches)
        .map_err(Error::CollectResults)?;
    Ok(web::Json(JsonValue::Array(
        data.into_iter().map(JsonValue::Object).collect(),
    )))
}

async fn setup_ctx() -> Result<SessionContext> {
    let target_partitions = 8;

    let cfg = SessionConfig::new()
        .with_target_partitions(target_partitions)
        .with_batch_size(2048);

    let ctx = SessionContext::with_config(cfg);

    let file_format = ParquetFormat::default().with_enable_pruning(true);
    let file_format = Arc::new(file_format);

    ctx.register_listing_table(
        "block",
        "file://../eth-archive/data/block/",
        ListingOptions {
            file_extension: ".parquet".to_owned(),
            format: file_format.clone(),
            table_partition_cols: Vec::new(),
            collect_stat: false,
            target_partitions,
        },
        None,
    )
    .await
    .map_err(Error::RegisterParquet)?;

    ctx.register_listing_table(
        "tx",
        "file://../eth-archive/data/tx/",
        ListingOptions {
            file_extension: ".parquet".to_owned(),
            format: file_format.clone(),
            table_partition_cols: Vec::new(),
            collect_stat: false,
            target_partitions,
        },
        None,
    )
    .await
    .map_err(Error::RegisterParquet)?;

    ctx.register_listing_table(
        "log",
        "file://../eth-archive/data/log/",
        ListingOptions {
            file_extension: ".parquet".to_owned(),
            format: file_format.clone(),
            table_partition_cols: Vec::new(),
            collect_stat: false,
            target_partitions,
        },
        None,
    )
    .await
    .map_err(Error::RegisterParquet)?;

    Ok(ctx)
}
