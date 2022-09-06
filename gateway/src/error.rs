use std::result::Result as StdResult;

use actix_web::{HttpResponse, ResponseError};
use std::io;
use thiserror::Error as ThisError;

#[derive(Debug, ThisError)]
pub enum Error {
    #[error("failed to read config file:\n{0}")]
    ReadConfigFile(std::io::Error),
    #[error("failed to parse config:\n{0}")]
    ParseConfig(toml::de::Error),
    #[error("failed to create database handle:\n{0}")]
    CreateDbHandle(Box<eth_archive_core::Error>),
    #[error("failed to execute query:\n{0}")]
    ExecuteQuery(datafusion::error::DataFusionError),
    #[error("failed to build query:\n{0}")]
    BuildQuery(datafusion::error::DataFusionError),
    #[error("failed to register parquet files to datafusion context:\n{0}")]
    RegisterParquet(datafusion::error::DataFusionError),
    #[error("failed to collect results of query:\n{0}")]
    CollectResults(arrow::error::ArrowError),
    #[error("no block found")]
    NoBlocks,
    #[error("invalid block number returned from query")]
    InvalidBlockNumber,
    #[error("at least one field has to be selected")]
    NoFieldsSelected,
    #[error("failed to apply address filters to query:\n{0}")]
    ApplyAddrFilters(datafusion::error::DataFusionError),
    #[error("failed to apply block range filter to query:\n{0}")]
    ApplyBlockRangeFilter(datafusion::error::DataFusionError),
    #[error("failed to apply sighash filter to query:\n{0}")]
    ApplySigHashFilter(datafusion::error::DataFusionError),
    #[error("failed to run http server:\n{0}")]
    RunHttpServer(io::Error),
    #[error("failed to bind http server:\n{0}")]
    BindHttpServer(io::Error),
    #[error("failed get minimum block number from database:\n{0}")]
    GetMinBlockNumber(eth_archive_core::Error),
    #[error("failed get maximum block number from database:\n{0}")]
    GetMaxBlockNumber(eth_archive_core::Error),
    #[error("maximum block range exceeded in query. max is {max} query had {range}.")]
    MaximumBlockRange { max: u32, range: u32 },
    #[error("failed to run sql query:\n{0}")]
    SqlQuery(eth_archive_core::Error),
    #[error("invalid hex in an address:\n{0}")]
    InvalidHexInAddress(prefix_hex::Error),
    #[error("invalid hex in a topic:\n{0}")]
    InvalidHexInTopic(prefix_hex::Error),
    #[error("failed to read parquet directory:\n{0}")]
    ReadParquetDir(io::Error),
    #[error("invalid parquet subdirectory name")]
    InvalidParquetSubdirectory,
    #[error("invalid block range in query")]
    InvalidBlockRange,
    #[error("failed to concatenate arrow data in memory:\n{0}")]
    ConcatRecordBatches(arrow::error::ArrowError),
    #[error("block range {0:?} not found in {1} parquet files")]
    RangeNotFoundInParquetFiles((u32, u32), &'static str),
    #[error("too many toics in query. maximum is 4 but got {0}")]
    TooManyTopics(usize),
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
