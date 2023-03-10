use crate::parquet_metadata::ParquetMetadata;
use crate::types::MiniQuery;
use crate::Result;
use eth_archive_core::dir_name::DirName;
use eth_archive_core::types::QueryResult;
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
