use std::io;
use std::result::Result as StdResult;

use thiserror::Error as ThisError;

#[derive(Debug, ThisError)]
pub enum Error {
    #[error("failed to parse rpc response:\n{0}")]
    RpcResponseParse(reqwest::Error),
    #[error("failed to parse rpc response json:\n{0}")]
    RpcResponseParseJson(serde_json::Error),
    #[error("failed to parse rpc result:\n{0}")]
    RpcResultParse(serde_json::Error),
    #[error("invalid rpc response")]
    InvalidRpcResponse,
    #[error("error: rpc response status is {0}. payload:\n{1:?}")]
    RpcResponseStatus(u16, Option<String>),
    #[error("failed to execute http request:\n{0}")]
    HttpRequest(reqwest::Error),
    #[error("failed to build http client:\n{0}")]
    BuildHttpClient(reqwest::Error),
    #[error("failed operation after retrying:\n{0:#?}")]
    Retry(Vec<Error>),
    #[error("invalid block range \"{0}\".")]
    InvalidBlockRange(String),
    #[error("failed to read parquet directory:\n{0}")]
    ReadParquetDir(io::Error),
    #[error("invalid folder name.")]
    InvalidFolderName,
    #[error("failed to get best block from ethereum node:\n{0:?}")]
    GetBestBlock(Vec<Error>),
    #[error("failed to delete temporary directory:\n{0}")]
    RemoveTempDir(io::Error),
}

pub type Result<T> = StdResult<T, Error>;
