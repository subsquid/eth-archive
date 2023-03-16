use crate::config::Config;
use crate::data_ctx::DataCtx;
use crate::error::{Error, Result};
use crate::types::Query;
use eth_archive_core::ingest_metrics::IngestMetrics;
use hyper::service::{make_service_fn, service_fn};
use hyper::{header, Body, Method, Request, Response, Server as HttpServer, StatusCode};
use std::convert::Infallible;
use std::sync::Arc;

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

        let make_service = make_service_fn(move |_| {
            let app_data = app_data.clone();
            async move { Ok::<_, Infallible>(service_fn(move |req| handler(app_data.clone(), req))) }
        });

        let server = HttpServer::bind(&server_addr)
            .http1_preserve_header_case(true)
            .http1_title_case_headers(true)
            .serve(make_service);

        server.await.map_err(Error::RunHttpServer)
    }
}

async fn handler(app_data: AppData, req: Request<Body>) -> hyper::Result<Response<Body>> {
    let res = match (req.method(), req.uri().path()) {
        (&Method::GET, "/ingest-metrics") => metrics_handler(app_data).await,
        (&Method::POST, "/query") => query_handler(app_data, req).await,
        (&Method::GET, "/height") => height_handler(app_data).await,
        _ => Ok(Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::empty())
            .unwrap()),
    };

    match res {
        Ok(res) => Ok(res),
        Err(e) => Ok(Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(Body::from(e.to_string()))
            .unwrap()),
    }
}

async fn metrics_handler(app_data: AppData) -> Result<Response<Body>> {
    let body = app_data
        .ingest_metrics
        .encode()
        .map_err(Error::EncodeMetrics)?;

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(
            header::CONTENT_TYPE,
            "application/openmetrics-text; version=1.0.0; charset=utf-8",
        )
        .body(Body::from(body))
        .unwrap())
}

async fn height_handler(app_data: AppData) -> Result<Response<Body>> {
    let height = app_data.data_ctx.inclusive_height();

    let json = simd_json::json!({ "height": height });

    let json = simd_json::to_string(&json).unwrap();

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(json))
        .unwrap())
}

async fn query_handler(app_data: AppData, req: Request<Body>) -> Result<Response<Body>> {
    let mut req = hyper::body::to_bytes(req.into_body())
        .await
        .map_err(|_| Error::InvalidRequestBody(None))?
        .to_vec();

    let query: Query =
        simd_json::from_slice(&mut req).map_err(|e| Error::InvalidRequestBody(Some(e)))?;

    let res = app_data.data_ctx.clone().query(query).await?;

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(res))
        .unwrap())
}
