use eth_archive_core::config::RetryConfig;
use serde::Deserialize;

#[derive(Deserialize)]
pub struct Config {
    pub db: DbConfig,
    pub ingest: IngestConfig,
    pub retry: RetryConfig,
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
}
