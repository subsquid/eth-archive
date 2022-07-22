use eth_archive_core::config::DbConfig;
use eth_archive_core::config::RetryConfig;
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
}

#[derive(Deserialize, Clone)]
pub struct ParquetConfig {
    pub name: String,
    pub items_per_file: usize,
    pub items_per_row_group: usize,
    pub path: PathBuf,
    pub channel_size: usize,
}

#[derive(Deserialize)]
pub struct IngestConfig {
    pub eth_rpc_url: url::Url,
    pub block_batch_size: usize,
    pub log_batch_size: usize,
    pub http_req_concurrency: usize,
}
