use std::result::Result as StdResult;
use thiserror::Error as ThisError;

#[derive(Debug, ThisError)]
pub enum Error {
    #[error("ethereum rpc request failed: {0}")]
    EthReq(web3::Error),
}

impl From<web3::Error> for Error {
    fn from(e: web3::Error) -> Error {
        Error::EthReq(e)
    }
}

pub type Result<T> = StdResult<T, Error>;
