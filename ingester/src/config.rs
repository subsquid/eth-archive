use eth_archive_core::config::DbConfig;
use eth_archive_core::config::RetryConfig;
use serde::Deserialize;

#[derive(Deserialize)]
pub struct Config {
    pub db: DbConfig,
    pub ingest: IngestConfig,
    pub retry: RetryConfig,
}

#[derive(Deserialize)]
pub struct IngestConfig {
    pub eth_rpc_url: url::Url,
}
