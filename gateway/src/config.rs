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

#[derive(Deserialize, Clone)]
pub struct DataConfig {
    pub blocks_path: String,
    pub transactions_path: String,
    pub logs_path: String,
    pub max_block_range: u32,
    pub default_block_range: u32,
    pub response_log_limit: usize,
    pub query_chunk_size: u32,
    pub query_time_limit_ms: u64,
    pub parquet_state_refresh_interval_secs: u64,
}

#[derive(Deserialize)]
pub struct HttpServerConfig {
    pub ip: Ipv4Addr,
    pub port: u16,
}
