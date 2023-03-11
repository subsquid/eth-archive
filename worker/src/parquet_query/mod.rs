use crate::parquet_metadata::ParquetMetadata;
use crate::types::{MiniQuery, QueryResult};
use crate::Result;
use eth_archive_core::dir_name::DirName;
use std::path::PathBuf;

pub struct ParquetQuery {
    pub data_path: PathBuf,
    pub dir_name: DirName,
    pub metadata: ParquetMetadata,
    pub mini_query: MiniQuery,
}

impl ParquetQuery {
    pub async fn run(self) -> Result<QueryResult> {
        todo!();
    }
}
