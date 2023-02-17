use crate::config::Config;
use crate::error::{Error, Result};
use crate::handler::Handler;
use crate::metrics::Metrics;
use crate::types::{RpcRequest, RpcResponse};
use std::sync::Arc;

use actix_web::{web, App, HttpResponse, HttpServer};

pub struct Server {}

#[derive(Clone)]
struct AppData {
    metrics: Arc<Metrics>,
    handler: Arc<Handler>,
}

impl Server {
    pub async fn run(config: Config) -> Result<()> {
        let metrics = Metrics::new();
        let metrics = Arc::new(metrics);

        let server_addr = config.server_addr;

        let handler = Handler::new(config, metrics.clone()).await?;
        let handler = Arc::new(handler);

        let app_data = AppData {
            metrics,
            handler,
        };

        HttpServer::new(move || {
            App::new()
                .app_data(web::Data::new(app_data.clone()))
                .service(web::resource("/").route(web::post().to(rpc_handler)))
                .service(web::resource("/metrics").route(web::get().to(metrics_handler)))
        })
        .bind(server_addr)
        .map_err(Error::BindHttpServer)?
        .run()
        .await
        .map_err(Error::RunHttpServer)
    }
}

async fn rpc_handler(
    req: web::Json<RpcRequest>,
    app_data: web::Data<AppData>,
) -> Result<web::Json<RpcResponse>> {
    let res = app_data.handler.clone().handle(req)?;

    Ok(HttpResponse::Ok().json(res))
}

async fn metrics_handler(app_data: web::Data<AppData>) -> Result<HttpResponse> {
    let body = app_data.metrics.encode().map_err(Error::EncodeMetrics)?;

    Ok(HttpResponse::Ok()
        .content_type("application/openmetrics-text; version=1.0.0; charset=utf-8")
        .body(body))
}
