use std::result::Result as StdResult;

use thiserror::Error as ThisError;

use std::io;

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
    CreateDbHandle(Box<eth_archive_core::Error>),
    #[error("failed to build ethereum rpc client:\n{0}")]
    CreateEthClient(eth_archive_core::Error),
    #[error("ethereum rpc client error:\n{0}")]
    EthClient(eth_archive_core::Error),
    #[error("failed to read parquet directory:\n{0}")]
    ReadParquetDir(io::Error),
    #[error("invalid parquet file name.")]
    InvalidParquetFileName,
    #[error("failed get minimum block number from database:\n{0}")]
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
