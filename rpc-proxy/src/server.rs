use crate::config::Config;
use crate::error::{Error, Result};
use crate::handler::Handler;
use crate::metrics::Metrics;
use crate::types::{MaybeBatch, RpcRequest};
use hyper::service::{make_service_fn, service_fn};
use hyper::{header, Body, Method, Request, Response, Server as HttpServer, StatusCode};
use std::convert::Infallible;
use std::sync::Arc;

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

        let req_handler = Handler::new(config, metrics.clone()).await?;
        let req_handler = Arc::new(req_handler);

        let app_data = AppData {
            metrics,
            handler: req_handler,
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
        (&Method::GET, "/metrics") => metrics_handler(app_data).await,
        (&Method::POST, "/") => rpc_handler(app_data, req).await,
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

async fn rpc_handler(app_data: AppData, req: Request<Body>) -> Result<Response<Body>> {
    let (parts, body) = req.into_parts();

    let req = hyper::body::to_bytes(body)
        .await
        .map_err(|_| Error::InvalidRequestBody(None))?;

    let rpc_req: MaybeBatch<RpcRequest> =
        serde_json::from_slice(req.as_ref()).map_err(|e| Error::InvalidRequestBody(Some(e)))?;

    let res = app_data
        .handler
        .clone()
        .handle(&parts.headers, rpc_req)
        .await?;

    let res = serde_json::to_string(&res).unwrap();

    Ok(Response::builder()
        .status(StatusCode::OK)
        .body(Body::from(res))
        .unwrap())
}

async fn metrics_handler(app_data: AppData) -> Result<Response<Body>> {
    let body = app_data.metrics.encode()?;

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(
            header::CONTENT_TYPE,
            "application/openmetrics-text; version=1.0.0; charset=utf-8",
        )
        .body(Body::from(body))
        .unwrap())
}
