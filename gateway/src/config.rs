use eth_archive_core::config::{DbConfig, RetryConfig};
use serde::Deserialize;
use std::net::Ipv4Addr;
use std::path::PathBuf;

#[derive(Deserialize)]
pub struct Config {
    pub retry: RetryConfig,
    pub db: DbConfig,
    pub datafusion: DatafusionConfig,
    pub http_server: HttpServerConfig,
}

#[derive(Deserialize)]
pub struct DatafusionConfig {
    pub target_partitions: usize,
    pub batch_size: usize,
    pub blocks_path: String,
    pub transactions_path: String,
    pub logs_path: String,
    pub max_block_range: usize,
}

#[derive(Deserialize)]
pub struct HttpServerConfig {
    pub ip: Ipv4Addr,
    pub port: u16,
}
