use std::io;
use std::result::Result as StdResult;
use thiserror::Error as ThisError;

#[derive(Debug, ThisError)]
pub enum Error {
    #[error("failed to build http client:\n{0}")]
    BuildHttpClient(reqwest::Error),
    #[error("failed operation after retrying:\n{0:#?}")]
    Retry(Vec<Error>),
}

pub type Result<T> = StdResult<T, Error>;
