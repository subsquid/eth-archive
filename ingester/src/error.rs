use std::error::Error as StdError;
use std::result::Result as StdResult;

use thiserror::Error as ThisError;

type Cause = Box<dyn StdError + Send + Sync>;

#[derive(Debug, ThisError)]
pub enum Error {
    #[error("failed to read config file:\n\t{0}")]
    ReadConfigFile(std::io::Error),
    #[error("failed to parse config:\n\t{0}")]
    ParseConfig(toml::de::Error),
    #[error("failed to create database handle:\n\t{0}")]
    CreateDbHandle(Box<Error>),
    #[error("failed to build database session:\n\t{0}")]
    BuildDbSession(scylla::transport::errors::NewSessionError),
    #[error("failed to initialize database schema:\n\t{0}")]
    InitSchema(Box<Error>),
    #[error("failed to reset database:\n\t{0}")]
    ResetDb(Box<Error>),
    #[error("failed to create keyspace:\n\t{0}")]
    CreateKeyspace(scylla::transport::errors::QueryError),
    #[error("failed to create block table:\n\t{0}")]
    CreateBlockTable(scylla::transport::errors::QueryError),
    #[error("failed to create transaction table:\n\t{0}")]
    CreateTxTable(scylla::transport::errors::QueryError),
    #[error("failed to create log table:\n\t{0}")]
    CreateLogTable(scylla::transport::errors::QueryError),
    #[error("failed to drop eth keyspace:\n\t{0}")]
    DropEthKeyspace(scylla::transport::errors::QueryError),
    #[error("failed to get maximum block number in database:\n\t{0}")]
    GetMaxBlockNumber(Cause),
    #[error("failed to get bestblock from ethereum node")]
    GetBestBlock,
    #[error("failed to build http client:\n\t{0}")]
    BuildHttpClient(reqwest::Error),
    #[error("failed to parse rpc response:\n\t{0}")]
    RpcResponseParse(reqwest::Error),
    #[error("failed to parse rpc result:\n\t{0}")]
    RpcResultParse(serde_json::Error),
    #[error("invalid rpc response")]
    RpcResponseInvalid,
    #[error("error: rpc response status is {0}")]
    RpcResponseStatus(u16),
    #[error("failed to execute http request:\n\t{0}")]
    HttpRequest(reqwest::Error),
    #[error("failed to insert blocks into database:\n\t{0}")]
    InsertBlocks(scylla::transport::errors::QueryError),
    #[error("failed to prepare insert block statement:\n\t{0}")]
    PrepareInsertBlockStatement(scylla::transport::errors::QueryError),
}

pub type Result<T> = StdResult<T, Error>;
