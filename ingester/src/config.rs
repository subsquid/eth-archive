use serde::Deserialize;

#[derive(Deserialize)]
pub struct Config {
    pub db: DbConfig,
    pub ingest: IngestConfig,
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
    pub from_block: Option<usize>,
    pub to_block: Option<usize>,
    pub tx_batch_size: usize,
    pub log_batch_size: usize,
    pub http_req_concurrency: usize,
    pub retry: RetryConfig,
}

#[derive(Deserialize, Clone, Copy)]
pub struct RetryConfig {
    pub num_tries: Option<usize>,
    pub secs_between_tries: u64,
}
