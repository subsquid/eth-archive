use eth_archive_core::config::{DbConfig, IngestConfig, RetryConfig};
use serde::Deserialize;
use std::path::PathBuf;

#[derive(Deserialize)]
pub struct Config {
    pub block: ParquetConfig,
    pub transaction: ParquetConfig,
    pub log: ParquetConfig,
    pub ingest: IngestConfig,
    pub retry: RetryConfig,
    pub db: DbConfig,
    pub block_overlap_size: usize,
}

#[derive(Deserialize, Clone)]
pub struct ParquetConfig {
    pub name: String,
    pub items_per_file: usize,
    pub items_per_row_group: usize,
    pub path: PathBuf,
    pub channel_size: usize,
}
