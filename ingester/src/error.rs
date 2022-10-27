use arrow2::error::Error as ArrowError;
use std::io;
use std::result::Result as StdResult;
use thiserror::Error as ThisError;

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
    #[error("folder range mismatch {0} => {1}")]
    FolderRangeMismatch(u32, u32),
    #[error("failed to run writer thread:\n{0}")]
    RunWriterThread(tokio::task::JoinError),
}

pub type Result<T> = StdResult<T, Error>;
