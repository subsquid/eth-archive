use std::result::Result as StdResult;

use thiserror::Error as ThisError;

#[derive(Debug, ThisError)]
pub enum Error {
    #[error("failed to read config file:\n{0}")]
    ReadConfigFile(std::io::Error),
    #[error("failed to parse config:\n{0}")]
    ParseConfig(toml::de::Error),
    #[error("failed to push element to arrow array:\n{0}")]
    PushRow(arrow2::error::ArrowError),
    #[error("failed to sort row group:\n{0}")]
    SortRowGroup(arrow2::error::ArrowError),
    #[error("no blocks in database yet. need to start parquet writer after ingester.")]
    NoBlocksInDb,
    #[error("failed to create database handle:\n{0}")]
    CreateDbHandle(Box<Error>),
    #[error("failed to create database connection pool:\n{0}")]
    CreatePool(deadpool_postgres::CreatePoolError),
    #[error("failed to get a database connection from pool:\n{0}")]
    GetDbConnection(deadpool_postgres::PoolError),
    #[error("failed to execute database query:\n{0}")]
    DbQuery(tokio_postgres::Error),
    #[error("failed to build ethereum rpc client:\n{0}")]
    CreateEthClient(eth_archive_core::Error),
    #[error("ethereum rpc client error:\n{0}")]
    EthClient(eth_archive_core::Error),
}

pub type Result<T> = StdResult<T, Error>;
