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
    #[error("failed to create database connection pool:\n{0}")]
    CreatePool(deadpool_postgres::CreatePoolError),
    #[error("failed to get a database connection from pool:\n{0}")]
    GetDbConnection(deadpool_postgres::PoolError),
    #[error("failed to run database migrations:\n{0}")]
    RunMigrations(refinery::Error),
    #[error("failed to reset database:\n{0}")]
    ResetDb(tokio_postgres::Error),
    #[error("failed to get bestblock from ethereum node")]
    GetBestBlock,
    #[error("failed to build http client:\n{0}")]
    BuildHttpClient(reqwest::Error),
    #[error("failed to parse rpc response:\n{0}")]
    RpcResponseParse(reqwest::Error),
    #[error("failed to parse rpc result:\n{0}")]
    RpcResultParse(serde_json::Error),
    #[error("invalid rpc response")]
    InvalidRpcResponse,
    #[error("error: rpc response status is {0}")]
    RpcResponseStatus(u16),
    #[error("failed to execute http request:\n{0}")]
    HttpRequest(reqwest::Error),
    #[error("failed to insert blocks:\n{0:#?}")]
    InsertBlocks(Vec<Error>),
    #[error("failed to decode block number:\n{0}")]
    DecodeBlockNumber(Cause),
    #[error("unexpected array length")]
    UnexpectedArrayLength,
}

pub type Result<T> = StdResult<T, Error>;
