use crate::config::Config;
use crate::data_ctx::DataCtx;
use crate::error::{Error, Result};
use crate::options::Options;
use crate::types::{Query, Status};
use eth_archive_core::db::DbHandle;
use std::sync::Arc;

use actix_web::{web, App, HttpResponse, HttpServer};

pub struct Server {}

impl Server {
    pub async fn run(options: Options) -> Result<()> {
        let config = Config::from(options);

        let db = DbHandle::new(false, &config.db)
            .await
            .map_err(|e| Error::CreateDbHandle(Box::new(e)))?;
        let db = Arc::new(db);

        let data_ctx = DataCtx::new(db, config.data).await?;
        let data_ctx = Arc::new(data_ctx);

        HttpServer::new(move || {
            App::new()
                .app_data(web::Data::new(data_ctx.clone()))
                .service(web::resource("/query").route(web::post().to(query)))
                .service(web::resource("/status").route(web::get().to(status)))
        })
        .bind((config.http_server.ip, config.http_server.port))
        .map_err(Error::BindHttpServer)?
        .run()
        .await
        .map_err(Error::RunHttpServer)
    }
}

async fn status(ctx: web::Data<Arc<DataCtx>>) -> Result<web::Json<Status>> {
    let status = ctx.status().await?;

    Ok(web::Json(status))
}

async fn query(query: web::Json<Query>, ctx: web::Data<Arc<DataCtx>>) -> Result<HttpResponse> {
    let res = ctx.query(query.into_inner()).await?;

    Ok(HttpResponse::Ok()
        .content_type("application/json")
        .body(res))
}
