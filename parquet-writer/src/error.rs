use std::io;
use std::result::Result as StdResult;
use thiserror::Error as ThisError;

#[derive(Debug, ThisError)]
pub enum Error {
    #[error("failed to read parquet directory:\n{0}")]
    ReadParquetDir(io::Error),
    #[error("invalid parquet file name.")]
    InvalidParquetFileName,
    #[error("invalid parquet file name \"{0}\"")]
    InvalidParquetFilename(String),
    #[error("failed to read parquet file name")]
    ReadParquetFileName,
    #[error("failed to create parquet file:\n{0}")]
    CreateParquetFile(io::Error),
}

pub type Result<T> = StdResult<T, Error>;
