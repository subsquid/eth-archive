use aws_sdk_s3::types::SdkError as S3Err;
use polars::error::ArrowError;
use std::result::Result as StdResult;
use std::string::FromUtf8Error;
use std::{fmt, io};
use thiserror::Error as ThisError;

type S3Put = S3Err<aws_sdk_s3::error::PutObjectError>;
type S3List = S3Err<aws_sdk_s3::error::ListObjectsV2Error>;
type S3Get = S3Err<aws_sdk_s3::error::GetObjectError>;

#[derive(Debug, ThisError)]
pub enum Error {
    #[error("failed to parse rpc response:\n{0}")]
    RpcResponseParse(reqwest::Error),
    #[error("failed to parse rpc response json:\n{0}")]
    RpcResponseParseJson(serde_json::Error),
    #[error("failed to parse rpc result:\n{0}")]
    RpcResultParse(serde_json::Error),
    #[error("invalid rpc response:\n{0}\nrequest was:\n{1}")]
    InvalidRpcResponse(String, String),
    #[error("error: rpc response status is {0}. payload:\n{1:?}")]
    RpcResponseStatus(u16, Option<String>),
    #[error("failed to execute http request:\n{0}")]
    HttpRequest(reqwest::Error),
    #[error("failed to build http client:\n{0}")]
    BuildHttpClient(reqwest::Error),
    #[error("failed operation after retrying:\n{0:#?}")]
    Retry(Vec<Error>),
    #[error("invalid block range \"{0}\".")]
    InvalidBlockRange(String),
    #[error("failed to read parquet directory:\n{0}")]
    ReadParquetDir(io::Error),
    #[error("invalid folder name.")]
    InvalidFolderName,
    #[error("failed to get best block from ethereum node:\n{0:?}")]
    GetBestBlock(Vec<Error>),
    #[error("failed to delete temporary directory:\n{0}")]
    RemoveTempDir(io::Error),
    #[error("failed to read ETH_RPC_URL from env:\n{0}")]
    ReadRpcUrlFromEnv(std::env::VarError),
    #[error("failed to parse ETH_RPC_URL:\n{0}")]
    ParseRpcUrl(url::ParseError),
    #[error("failed to encode metrics:\n{0}")]
    EncodeMetrics(fmt::Error),
    #[error("encoded metrics have invalid utf8:\n{0}")]
    MetricsUtf8(FromUtf8Error),
    #[error("failed to put object to s3:\n{0}")]
    S3Put(S3Put),
    #[error("failed to list objects in s3:\n{0}")]
    S3List(S3List),
    #[error("failed get object from s3:\n{0}")]
    S3Get(S3Get),
    #[error("failed to create missing directories:\n{0}")]
    CreateMissingDirectories(io::Error),
    #[error("failed to open file:\n{0}")]
    OpenFile(io::Error),
    #[error("failed to get object chunk when downloading from s3.")]
    S3GetObjChunk,
    #[error("failed to write file:\n{0}")]
    WriteFile(io::Error),
    #[error("failed to read file:\n{0}")]
    ReadFile(io::Error),
    #[error("failed to check if parquet directory is valid.")]
    CheckParquetDir,
    #[error("failed to rename file:\n{0}")]
    RenameFile(io::Error),
    #[error("no healthy rpc url found.")]
    NoHealthyUrl,
    #[error("failed to read parquet file:\n{0}")]
    ReadParquet(ArrowError),
    #[error("block {0} not found while streaming batches from s3")]
    BlockNotFoundInS3(u32),
    #[error("unknown format: {0}")]
    UnknownFormat(String),
    #[error("no blocks to ingest from rpc node.")]
    NoBlocksOnNode,
}

pub type Result<T> = StdResult<T, Error>;
