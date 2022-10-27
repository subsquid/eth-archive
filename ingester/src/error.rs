use arrow2::error::Error as ArrowError;
use std::result::Result as StdResult;
use thiserror::Error as ThisError;
use std::io;

#[derive(Debug, ThisError)]
pub enum Error {
    #[error("failed to sort row group:\n{0}")]
    SortRowGroup(ArrowError),
    #[error("failed to build ethereum rpc client:\n{0}")]
    CreateEthClient(eth_archive_core::Error),
    #[error("ethereum rpc client error:\n{0}")]
    EthClient(eth_archive_core::Error),
    #[error("invalid parquet file name.")]
    GetMinBlockNumber(eth_archive_core::Error),
    #[error("failed to get logs from database:\n{0}")]
    GetLogsFromDb(eth_archive_core::Error),
    #[error("failed to get best block from ethereum node:\n{0}")]
    GetBestBlock(eth_archive_core::Error),
    #[error("failed to get data batch from ethereum node:\n{0}")]
    GetBatch(eth_archive_core::Error),
    #[error("failed to list folder names:\n{0}")]
    ListFolderNames(eth_archive_core::Error),
    #[error("failed to create missing directories:\n{0}")]
    CreateMissingDirectories(io::Error),
    #[error("failed to delete temporary directory:\n{0}")]
    RemoveTempDir(io::Error),
}

pub type Result<T> = StdResult<T, Error>;
