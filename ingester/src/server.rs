use crate::error::{Error, Result};
use eth_archive_core::ingest_metrics::IngestMetrics;
use std::net::SocketAddr;
use std::sync::Arc;

use actix_web::{web, App, HttpResponse, HttpServer};

pub struct Server {}

impl Server {
    pub async fn run(addr: SocketAddr, metrics: Arc<IngestMetrics>) -> Result<()> {
        let listener = TcpListener::bind(addr)
            .await
            .map_err(Error::BindHttpServer)?;

        loop {
            let (stream, _) = listener.accept().await.map_err(Error::RunHttpServer)?;
            tokio::task::spawn(async move {
                if let Err(err) = http1::Builder::new()
                    .preserve_header_case(true)
                    .title_case_headers(true)
                    .serve_connection(stream, service_fn(proxy))
                    .with_upgrades()
                    .await
                {
                    println!("Failed to serve connection: {:?}", err);
                }
            });
        }

        Ok(())
    }
}

async fn metrics_handler(metrics: web::Data<Arc<IngestMetrics>>) -> Result<HttpResponse> {
    let body = metrics.encode().map_err(Error::EncodeMetrics)?;

    Ok(HttpResponse::Ok()
        .content_type("application/openmetrics-text; version=1.0.0; charset=utf-8")
        .body(body))
}
