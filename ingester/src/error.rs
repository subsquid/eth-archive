use std::result::Result as StdResult;

use thiserror::Error as ThisError;

#[derive(Debug, ThisError)]
pub enum Error {
    #[error("failed to read config file:\n{0}")]
    ReadConfigFile(std::io::Error),
    #[error("failed to parse config:\n{0}")]
    ParseConfig(toml::de::Error),
    #[error("failed to create database handle:\n{0}")]
    CreateDbHandle(Box<eth_archive_core::Error>),
    #[error("failed to get bestblock from ethereum node")]
    GetBestBlock,
    #[error("failed to crate ethereum rpc client for ingestion:\n{0}")]
    CreateEthClient(eth_archive_core::Error),
    #[error("ethereum rpc client error:\n{0}")]
    EthClient(eth_archive_core::Error),
    #[error("block window size is bigger than bestblock acquired from eth node")]
    BlockWindowBiggerThanBestblock,
    #[error("failed to get block from ethereum rpc:\n{0:#?}")]
    GetBlock(Vec<Error>),
    #[error("failed get minimum block number from database:\n{0}")]
    GetMinBlockNumber(eth_archive_core::Error),
    #[error("failed get maximum block number from database:\n{0}")]
    GetMaxBlockNumber(eth_archive_core::Error),
    #[error("failed to insert blocks to database:\n{0}")]
    InsertBlocks(eth_archive_core::Error),
    #[error("failed operation after retrying:\n{0:#?}")]
    Retry(Vec<Error>),
    #[error("failed to join tokio task:\n{0}")]
    JoinError(tokio::task::JoinError),
}

pub type Result<T> = StdResult<T, Error>;
