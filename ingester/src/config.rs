use eth_archive_core::config::{DbConfig, IngestConfig, RetryConfig};
use serde::Deserialize;

#[derive(Deserialize, Clone, Debug)]
pub struct Config {
    pub db: DbConfig,
    pub ingest: IngestConfig,
    pub retry: RetryConfig,
    pub block_window_size: usize,
}
