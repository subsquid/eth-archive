use arrow2::error::Error as ArrowError;
use std::io;
use std::result::Result as StdResult;
use thiserror::Error as ThisError;

#[derive(Debug, ThisError)]
pub enum Error {
    #[error("no block found")]
    NoBlocks,
    #[error("invalid block number returned from query")]
    InvalidBlockNumber,
    #[error("at least one field has to be selected")]
    NoFieldsSelected,
    #[error("failed to run http server:\n{0}")]
    RunHttpServer(hyper::Error),
    #[error("failed to run sql query:\n{0}")]
    SqlQuery(eth_archive_core::Error),
    #[error("invalid hex in an address:\n{0}")]
    InvalidHexInAddress(prefix_hex::Error),
    #[error("invalid hex in a topic:\n{0}")]
    InvalidHexInTopic(prefix_hex::Error),
    #[error("failed to read parquet directory:\n{0}")]
    ReadParquetDir(io::Error),
    #[error("invalid block range in query")]
    InvalidBlockRange,
    #[error("invalid address in query")]
    InvalidAddress,
    #[error("invalid topic in query")]
    InvalidTopic,
    #[error("block range {0:?} not found in {1} parquet files")]
    RangeNotFoundInParquetFiles((u32, u32), &'static str),
    #[error("too many toics in query. maximum is 4 but got {0}")]
    TooManyTopics(usize),
    #[error("invalid parquet file name \"{0}\"")]
    InvalidParquetFilename(String),
    #[error("failed to read parquet file name")]
    ReadParquetFileName,
    #[error("failed to join async task:\n{0}")]
    TaskJoinError(tokio::task::JoinError),
    #[error("failed to open database:\n{0}")]
    OpenDb(rocksdb::Error),
    #[error("database error:\n{0}")]
    Db(rocksdb::Error),
    #[error("failed to build ethereum rpc client:\n{0}")]
    CreateEthClient(eth_archive_core::Error),
    #[error("failed to encode metrics:\n{0}")]
    EncodeMetrics(eth_archive_core::Error),
    #[error("failed to build s3 client:\n{0}")]
    BuildS3Client(eth_archive_core::Error),
    #[error("empty query")]
    EmptyQuery,
    #[error("max number of queries are running.")]
    MaxNumberOfQueriesReached,
    #[error("invalid request body:\n{0:?}")]
    InvalidRequestBody(Option<serde_json::Error>),
    #[error("failed to create missing directories:\n{0}")]
    CreateMissingDirectories(io::Error),
    #[error("failed to get best block:\n{0}")]
    GetBestBlock(eth_archive_core::Error),
    #[error("failed to read parquet file:\n{0}")]
    ReadParquet(ArrowError),
    #[error("failed to open file:\n{0}")]
    OpenParquetFile(io::Error),
}

pub type Result<T> = StdResult<T, Error>;
