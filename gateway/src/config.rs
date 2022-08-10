use eth_archive_core::config::{DbConfig, RetryConfig};
use serde::Deserialize;
use std::net::Ipv4Addr;

#[derive(Deserialize)]
pub struct Config {
    pub retry: RetryConfig,
    pub db: DbConfig,
    pub data: DataConfig,
    pub http_server: HttpServerConfig,
}

#[derive(Deserialize)]
pub struct DataConfig {
    pub target_partitions: usize,
    pub batch_size: usize,
    pub blocks_path: String,
    pub transactions_path: String,
    pub logs_path: String,
    pub max_block_range: usize,
    pub parquet_ctx_refresh_interval_secs: usize,
}

#[derive(Deserialize)]
pub struct HttpServerConfig {
    pub ip: Ipv4Addr,
    pub port: u16,
}
