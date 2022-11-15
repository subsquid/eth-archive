use crate::error::{Error, Result};
use eth_archive_core::ingest_metrics::IngestMetrics;
use std::net::SocketAddr;
use std::sync::Arc;

use actix_web::{web, App, HttpResponse, HttpServer};

pub struct Server {}

impl Server {
    pub async fn run(addr: SocketAddr, metrics: Arc<IngestMetrics>) -> Result<()> {
        HttpServer::new(move || {
            App::new()
                .app_data(web::Data::new(metrics.clone()))
                .service(web::resource("/metrics").route(web::get().to(metrics_handler)))
        })
        .disable_signals()
        .bind(addr)
        .map_err(Error::BindHttpServer)?
        .run()
        .await
        .map_err(Error::RunHttpServer)
    }
}

async fn metrics_handler(metrics: web::Data<Arc<IngestMetrics>>) -> Result<HttpResponse> {
    let body = metrics.encode().map_err(Error::EncodeMetrics)?;

    Ok(HttpResponse::Ok()
        .content_type("application/openmetrics-text; version=1.0.0; charset=utf-8")
        .body(body))
}
