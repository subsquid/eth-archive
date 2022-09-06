use serde::Deserialize;
use std::num::NonZeroU64;

#[derive(Deserialize, Clone, Copy, Debug)]
pub struct RetryConfig {
    pub num_tries: Option<usize>,
    pub secs_between_tries: u64,
}

#[derive(Deserialize, Clone, Debug)]
pub struct DbConfig {
    pub user: String,
    pub password: String,
    pub dbname: String,
    pub host: String,
    pub port: u16,
}

#[derive(Deserialize, Clone, Debug)]
pub struct IngestConfig {
    pub eth_rpc_url: url::Url,
    pub request_timeout_secs: NonZeroU64,
    pub connect_timeout_ms: NonZeroU64,
    pub block_batch_size: usize,
    pub http_req_concurrency: usize,
}
