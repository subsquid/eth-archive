use actix_web::http::header::ToStrError;
use actix_web::{HttpResponse, ResponseError};
use std::result::Result as StdResult;
use std::{fmt, io};
use thiserror::Error as ThisError;

#[derive(Debug, ThisError)]
pub enum Error {
    #[error("the request has been rate limited.")]
    RateLimited,
    #[error("no endpoint has been specified.")]
    NoEndpointSpecified,
    #[error("invalid value in header {0}:\n{1}")]
    InvalidHeaderValue(&'static str, ToStrError),
    #[error("failed to encode metrics:\n{0}")]
    EncodeMetrics(fmt::Error),
    #[error("failed to run http server:\n{0}")]
    RunHttpServer(io::Error),
    #[error("failed to bind http server:\n{0}")]
    BindHttpServer(io::Error),
    #[error("failed to build http client:\n{0}")]
    BuildHttpClient(reqwest::Error),
    #[error("failed operation after retrying:\n{0:#?}")]
    Retry(Vec<Error>),
    #[error("failed to execute http request:\n{0}")]
    HttpRequest(reqwest::Error),
    #[error("invalid rpc response:\n{0}\nrequest was:\n{1}")]
    InvalidRpcResponse(String, String),
    #[error("error: rpc response status is {0}. payload:\n{1:?}")]
    RpcResponseStatus(u16, Option<String>),
    #[error("failed to parse rpc response:\n{0}")]
    RpcResponseParse(reqwest::Error),
}

pub type Result<T> = StdResult<T, Error>;

impl ResponseError for Error {
    fn error_response(&self) -> HttpResponse {
        log::debug!("error while serving request:\n{}", self);

        HttpResponse::InternalServerError().json(serde_json::json!({
            "error": self.to_string(),
        }))
    }
}
