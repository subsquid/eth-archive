use eth_archive_core::config::RetryConfig;
use serde::Deserialize;

#[derive(Deserialize)]
pub struct Config {
    block: ParquetConfig,
    transaction: ParquetConfig,
    log: ParquetConfig,
    ingest: IngestConfig,
    retry: RetryConfig,
}

#[derive(Deserialize)]
pub struct ParquetConfig {
    pub items_per_file: usize,
    pub items_per_row_group: usize,
}

#[derive(Deserialize)]
pub struct DbConfig {
    pub user: String,
    pub password: String,
    pub dbname: String,
    pub host: String,
    pub port: u16,
}

#[derive(Deserialize)]
pub struct IngestConfig {
    pub eth_rpc_url: url::Url,
    pub block_batch_size: usize,
    pub log_batch_size: usize,
    pub http_req_concurrency: usize,
}
