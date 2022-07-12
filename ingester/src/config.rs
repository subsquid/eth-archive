use scylla::transport::Compression as ScyllaCompression;
use serde::Deserialize;

#[derive(Deserialize)]
pub struct Config {
    pub db: DbConfig,
    pub ingest: IngestConfig,
}

#[derive(Deserialize)]
pub struct DbConfig {
    pub auth: Option<DbAuth>,
    pub connection_compression: Option<Compression>,
    pub known_nodes: Vec<String>,
}

#[derive(Deserialize)]
pub struct DbAuth {
    pub username: String,
    pub password: String,
}

#[derive(Deserialize, Clone, Copy)]
pub enum Compression {
    Lz4,
    Snappy,
}

impl From<Compression> for ScyllaCompression {
    fn from(compression: Compression) -> ScyllaCompression {
        match compression {
            Compression::Lz4 => ScyllaCompression::Lz4,
            Compression::Snappy => ScyllaCompression::Snappy,
        }
    }
}

#[derive(Deserialize)]
pub struct IngestConfig {
    pub eth_rpc_url: url::Url,
    pub from_block: Option<u64>,
    pub to_block: Option<u64>,
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
