use arrow2::error::Error as ArrowError;
use std::result::Result as StdResult;
use thiserror::Error as ThisError;

#[derive(Debug, ThisError)]
pub enum Error {
    #[error("failed to sort row group:\n{0}")]
    SortRowGroup(ArrowError),
    #[error("failed to create database handle:\n{0}")]
    CreateDbHandle(Box<eth_archive_core::Error>),
    #[error("failed to build ethereum rpc client:\n{0}")]
    CreateEthClient(eth_archive_core::Error),
    #[error("ethereum rpc client error:\n{0}")]
    EthClient(eth_archive_core::Error),
    #[error("invalid parquet file name.")]
    GetMinBlockNumber(eth_archive_core::Error),
    #[error("failed to get block from database:\n{0}")]
    GetBlockFromDb(eth_archive_core::Error),
    #[error("failed to get blocks from database:\n{0}")]
    GetBlocksFromDb(eth_archive_core::Error),
    #[error("failed to get transactions from database:\n{0}")]
    GetTxsFromDb(eth_archive_core::Error),
    #[error("failed to get logs from database:\n{0}")]
    GetLogsFromDb(eth_archive_core::Error),
}

pub type Result<T> = StdResult<T, Error>;
