use crate::error::{Error, Result};
use eth_archive_core::ingest_metrics::IngestMetrics;
use hyper::service::{make_service_fn, service_fn};
use hyper::{header, Body, Method, Request, Response, Server as HttpServer, StatusCode};
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;

pub struct Server {}

impl Server {
    pub async fn run(addr: SocketAddr, metrics: Arc<IngestMetrics>) -> Result<()> {
        let make_service = make_service_fn(move |_| {
            let metrics = metrics.clone();
            async move { Ok::<_, Infallible>(service_fn(move |req| handler(metrics.clone(), req))) }
        });

        let server = HttpServer::bind(&addr)
            .http1_preserve_header_case(true)
            .http1_title_case_headers(true)
            .serve(make_service);

        server.await.map_err(Error::RunHttpServer)
    }
}

async fn handler(metrics: Arc<IngestMetrics>, req: Request<Body>) -> hyper::Result<Response<Body>> {
    let res = match (req.method(), req.uri().path()) {
        (&Method::GET, "/metrics") => metrics_handler(metrics).await,
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

async fn metrics_handler(metrics: Arc<IngestMetrics>) -> Result<Response<Body>> {
    let body = metrics.encode().map_err(Error::EncodeMetrics)?;

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(
            header::CONTENT_TYPE,
            "application/openmetrics-text; version=1.0.0; charset=utf-8",
        )
        .body(Body::from(body))
        .unwrap())
}
