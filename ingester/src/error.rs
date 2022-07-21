use std::result::Result as StdResult;

use thiserror::Error as ThisError;

#[derive(Debug, ThisError)]
pub enum Error {
    #[error("failed to read config file:\n{0}")]
    ReadConfigFile(std::io::Error),
    #[error("failed to parse config:\n{0}")]
    ParseConfig(toml::de::Error),
    #[error("failed to create database handle:\n{0}")]
    CreateDbHandle(Box<Error>),
    #[error("failed to create database connection pool:\n{0}")]
    CreatePool(deadpool_postgres::CreatePoolError),
    #[error("failed to get a database connection from pool:\n{0}")]
    GetDbConnection(deadpool_postgres::PoolError),
    #[error("failed to reset database:\n{0}")]
    ResetDb(tokio_postgres::Error),
    #[error("failed to initialize database tables:\n{0}")]
    InitDb(tokio_postgres::Error),
    #[error("failed to get bestblock from ethereum node")]
    GetBestBlock,
    #[error("failed to insert block to database:\n{0:#?}")]
    InsertBlock(tokio_postgres::Error),
    #[error("failed to execute database query:\n{0}")]
    DbQuery(tokio_postgres::Error),
    #[error("failed to create database transaction:\n{0}")]
    CreateDbTransaction(tokio_postgres::Error),
    #[error("failed to commit database transaction:\n{0}")]
    CommitDbTx(tokio_postgres::Error),
    #[error("failed to crate ethereum rpc client for ingestion:\n{0}")]
    CreateEthClient(eth_archive_core::Error),
    #[error("ethereum rpc client error:\n{0}")]
    EthClient(eth_archive_core::Error),
    #[error("block window size is bigger than bestblock acquired from eth node")]
    BlockWindowBiggerThanBestblock,
    #[error("failed to get block from ethereum rpc:\n{0:#?}")]
    GetBlock(Vec<Error>),
}

pub type Result<T> = StdResult<T, Error>;
