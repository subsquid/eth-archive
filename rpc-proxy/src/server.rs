use crate::config::Config;
use crate::data_ctx::DataCtx;
use crate::error::{Error, Result};
use crate::types::Query;
use eth_archive_core::ingest_metrics::IngestMetrics;
use std::sync::Arc;

use actix_web::{web, App, HttpResponse, HttpServer};

pub struct Server {}

#[derive(Clone)]
struct AppData {
    ingest_metrics: Arc<IngestMetrics>,
    data_ctx: Arc<DataCtx>,
}

impl Server {
    pub async fn run(config: Config) -> Result<()> {
        let ingest_metrics = IngestMetrics::new();
        let ingest_metrics = Arc::new(ingest_metrics);

        let server_addr = config.server_addr;

        let data_ctx = DataCtx::new(config, ingest_metrics.clone()).await?;
        let data_ctx = Arc::new(data_ctx);

        let app_data = AppData {
            ingest_metrics,
            data_ctx,
        };

        HttpServer::new(move || {
            App::new()
                .app_data(web::Data::new(app_data.clone()))
                .service(web::resource("/query").route(web::post().to(query)))
                .service(web::resource("/height").route(web::get().to(height)))
                .service(
                    web::resource("/ingest-metrics").route(web::get().to(ingest_metrics_handler)),
                )
        })
        .bind(server_addr)
        .map_err(Error::BindHttpServer)?
        .run()
        .await
        .map_err(Error::RunHttpServer)
    }
}

async fn height(app_data: web::Data<AppData>) -> Result<web::Json<serde_json::Value>> {
    let height = app_data.data_ctx.inclusive_height();

    Ok(web::Json(serde_json::json!({ "height": height })))
}

async fn query(query: web::Json<Query>, app_data: web::Data<AppData>) -> Result<HttpResponse> {
    let res = app_data.data_ctx.clone().query(query.into_inner()).await?;

    Ok(HttpResponse::Ok()
        .content_type("application/json")
        .body(res))
}

async fn ingest_metrics_handler(app_data: web::Data<AppData>) -> Result<HttpResponse> {
    let body = app_data
        .ingest_metrics
        .encode()
        .map_err(Error::EncodeMetrics)?;

    Ok(HttpResponse::Ok()
        .content_type("application/openmetrics-text; version=1.0.0; charset=utf-8")
        .body(body))
}
