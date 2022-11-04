use crate::config::Config;
use crate::data_ctx::DataCtx;
use crate::error::{Error, Result};
use crate::types::Query;
use std::sync::Arc;

use actix_web::{web, App, HttpResponse, HttpServer};

pub struct Server {}

impl Server {
    pub async fn run(config: Config) -> Result<()> {
        let data_ctx = DataCtx::new(config).await?;
        let data_ctx = Arc::new(data_ctx);

        HttpServer::new(move || {
            App::new()
                .app_data(web::Data::new(data_ctx.clone()))
                .service(web::resource("/query").route(web::post().to(query)))
                .service(web::resource("/height").route(web::get().to(height)))
        })
        .bind(config.server_addr)
        .map_err(Error::BindHttpServer)?
        .run()
        .await
        .map_err(Error::RunHttpServer)
    }
}

async fn height(ctx: web::Data<Arc<DataCtx>>) -> Result<web::Json<serde_json::Value>> {
    let height = ctx.height();

    Ok(web::Json(serde_json::json!({
        "height": height
    })))
}

async fn query(query: web::Json<Query>, ctx: web::Data<Arc<DataCtx>>) -> Result<HttpResponse> {
    let res = ctx.query(query.into_inner()).await?;

    Ok(HttpResponse::Ok()
        .content_type("application/json")
        .body(res))
}
