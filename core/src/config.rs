use serde::Deserialize;

#[derive(Deserialize, Clone, Copy, Debug)]
pub struct RetryConfig {
    pub num_tries: Option<usize>,
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
    pub block_batch_size: usize,
    pub http_req_concurrency: usize,
}
