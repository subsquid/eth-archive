use std::result::Result as StdResult;

use actix_web::{HttpResponse, ResponseError};
use polars::error::PolarsError;
use std::io;
use thiserror::Error as ThisError;

#[derive(Debug, ThisError)]
pub enum Error {
    #[error("failed to create database handle:\n{0}")]
    CreateDbHandle(Box<eth_archive_core::Error>),
    #[error("failed to execute query:\n{0}")]
    ExecuteQuery(PolarsError),
    #[error("failed to build query:\n{0}")]
    BuildQuery(PolarsError),
    #[error("failed to collect results of query:\n{0}")]
    CollectResults(PolarsError),
    #[error("no block found")]
    NoBlocks,
    #[error("invalid block number returned from query")]
    InvalidBlockNumber,
    #[error("at least one field has to be selected")]
    NoFieldsSelected,
    #[error("failed to apply address filters to query:\n{0}")]
    ApplyAddrFilters(PolarsError),
    #[error("failed to apply block range filter to query:\n{0}")]
    ApplyBlockRangeFilter(PolarsError),
    #[error("failed to run http server:\n{0}")]
    RunHttpServer(io::Error),
    #[error("failed to bind http server:\n{0}")]
    BindHttpServer(io::Error),
    #[error("failed get minimum block number from database:\n{0}")]
    GetMinBlockNumber(eth_archive_core::Error),
    #[error("failed get maximum block number from database:\n{0}")]
    GetMaxBlockNumber(eth_archive_core::Error),
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
    #[error("missing {0} in sorted ranges")]
    MissingEntryInSortedRanges(u32),
    #[error("failed to create range map from filenames:\n{0}")]
    CreateRangeMap(Box<Error>),
    #[error("invalid parquet file name \"{0}\"")]
    InvalidParquetFilename(String),
    #[error("failed to read parquet file name")]
    ReadParquetFileName,
    #[error("failed to scan for parquet file:\n{0}")]
    ScanParquet(PolarsError),
    #[error("failed to union data frames:\n{0}")]
    UnionFrames(PolarsError),
    #[error("failed to get column from result frame:\n{0}")]
    GetColumn(PolarsError),
    #[error("failed to join async task\n{0}")]
    TaskJoinError(tokio::task::JoinError),
}

pub type Result<T> = StdResult<T, Error>;

impl ResponseError for Error {
    fn error_response(&self) -> HttpResponse {
        log::debug!("error while serving request:\n{}", self);

        HttpResponse::InternalServerError().json(serde_json::json!({
            "error": self.to_string(),
        }))
    }
}