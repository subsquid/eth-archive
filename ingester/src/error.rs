use std::error::Error as StdError;
use std::result::Result as StdResult;

use thiserror::Error as ThisError;

type Cause = Box<dyn StdError + Send + Sync>;

#[derive(Debug, ThisError)]
pub enum Error {
    #[error("failed to read config file:\n{0}")]
    ReadConfigFile(std::io::Error),
    #[error("failed to parse config:\n{0}")]
    ParseConfig(toml::de::Error),
    #[error("failed to create database handle:\n{0}")]
    CreateDbHandle(Box<Error>),
    #[error("failed to build database session:\n{0}")]
    BuildDbSession(scylla::transport::errors::NewSessionError),
    #[error("failed to initialize database schema:\n{0}")]
    InitSchema(Box<Error>),
    #[error("failed to reset database:\n{0}")]
    ResetDb(Box<Error>),
    #[error("failed to create keyspace:\n{0}")]
    CreateKeyspace(scylla::transport::errors::QueryError),
    #[error("failed to create block table:\n{0}")]
    CreateBlockTable(scylla::transport::errors::QueryError),
    #[error("failed to create transaction table:\n{0}")]
    CreateTxTable(scylla::transport::errors::QueryError),
    #[error("failed to create log table:\n{0}")]
    CreateLogTable(scylla::transport::errors::QueryError),
    #[error("failed to drop eth keyspace:\n{0}")]
    DropEthKeyspace(scylla::transport::errors::QueryError),
    #[error("failed to get maximum block number in database:\n{0}")]
    GetMaxBlockNumber(Cause),
    #[error("failed to get bestblock from ethereum node")]
    GetBestBlock,
    #[error("failed to build http client:\n{0}")]
    BuildHttpClient,
}

pub type Result<T> = StdResult<T, Error>;
