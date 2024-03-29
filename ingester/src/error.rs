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
    #[error("failed to get data batch from s3:\n{0}")]
    GetS3Batch(eth_archive_core::Error),
    #[error("failed to get data batch from local file system:\n{0}")]
    GetLocalBatch(eth_archive_core::Error),
    #[error("failed to list folder names:\n{0}")]
    ListFolderNames(eth_archive_core::Error),
    #[error("failed to create missing directories:\n{0}")]
    CreateMissingDirectories(io::Error),
    #[error("folder range mismatch {0} => {1}")]
    FolderRangeMismatch(u32, u32),
    #[error("failed to run writer thread:\n{0}")]
    RunWriterThread(tokio::task::JoinError),
    #[error("failed to create directory:\n{0}")]
    CreateDir(io::Error),
    #[error("failed to rename directory:\n{0}")]
    RenameDir(io::Error),
    #[error("failed to create file:\n{0}")]
    CreateFile(io::Error),
    #[error("failed to write file data:\n{0}")]
    WriteFileData(ArrowError),
    #[error("failed to create file sink:\n{0}")]
    CreateFileSink(ArrowError),
    #[error("failed to close file sink:\n{0}")]
    CloseFileSink(ArrowError),
    #[error("failed to encode metrics:\n{0}")]
    EncodeMetrics(eth_archive_core::Error),
    #[error("failed to run http server:\n{0}")]
    RunHttpServer(hyper::Error),
    #[error("failed to read parquet file:\n{0}")]
    ReadParquet(arrow2::error::Error),
    #[error("failed operation after retrying:\n{0:#?}")]
    Retry(Vec<Error>),
    #[error("failed to build s3 client:\n{0}")]
    BuildS3Client(eth_archive_core::Error),
    #[error("failed to start streaming data from s3:\n{0}")]
    StartS3BatchStream(eth_archive_core::Error),
    #[error("failed to start streaming data from local file system:\n{0}")]
    StartLocalBatchStream(eth_archive_core::Error),
}

pub type Result<T> = StdResult<T, Error>;
