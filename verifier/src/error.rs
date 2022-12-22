use std::io;
use std::result::Result as StdResult;
use thiserror::Error as ThisError;

#[derive(Debug, ThisError)]
pub enum Error {
    #[error("failed to build http client:\n{0}")]
    BuildHttpClient(reqwest::Error),
    #[error("failed operation after retrying:\n{0:#?}")]
    Retry(Vec<Error>),
    #[error("failed to execute http request:\n{0}")]
    HttpRequest(reqwest::Error),
    #[error("failed to parse archive response:\n{0}")]
    ArchiveResponseParse(reqwest::Error),
    #[error("error: archive response status is {0}. payload:\n{1:?}")]
    ArchiveResponseStatus(u16, Option<String>),
    #[error("failed to create archive client:\n{0}")]
    CreateArchiveClient(Box<Error>),
    #[error("failed to build ethereum rpc client:\n{0}")]
    CreateEthClient(eth_archive_core::Error),
}

pub type Result<T> = StdResult<T, Error>;
