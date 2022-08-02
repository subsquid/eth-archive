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
    #[error("failed to create database connection pool:\n{0}")]
    CreatePool(deadpool_postgres::CreatePoolError),
    #[error("failed to get a database connection from pool:\n{0}")]
    GetDbConnection(deadpool_postgres::PoolError),
    #[error("failed to execute database query:\n{0}")]
    DbQuery(tokio_postgres::Error),
    #[error("failed to reset database:\n{0}")]
    ResetDb(tokio_postgres::Error),
    #[error("failed to initialize database tables:\n{0}")]
    InitDb(tokio_postgres::Error),
    #[error("failed to create database transaction:\n{0}")]
    CreateDbTransaction(tokio_postgres::Error),
    #[error("failed to commit database transaction:\n{0}")]
    CommitDbTx(tokio_postgres::Error),
    #[error("failed to insert blocks to database:\n{0}")]
    InsertBlocks(tokio_postgres::Error),
    #[error("failed to insert transactions to database:\n{0}")]
    InsertTransactions(tokio_postgres::Error),
    #[error("failed operation after retrying:\n{0:#?}")]
    Retry(Vec<Error>),
}

pub type Result<T> = StdResult<T, Error>;
