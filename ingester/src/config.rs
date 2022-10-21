use crate::Options;
use eth_archive_core::config::{DbConfig, IngestConfig, RetryConfig};
use serde::Deserialize;

#[derive(Deserialize, Clone, Debug)]
pub struct Config {
    pub db: DbConfig,
    pub ingest: IngestConfig,
    pub retry: RetryConfig,
    pub block_window_size: usize,
}

impl From<Options> for Config {
    fn from(options: Options) -> Self {
        Config {
            db: DbConfig {
                user: options.db_user,
                password: options.db_password,
                host: options.db_host,
                port: options.db_port,
                dbname: options.db_name,
            },
            ingest: IngestConfig {
                eth_rpc_url: options.eth_rpc_url,
                block_batch_size: options.block_batch_size,
                http_req_concurrency: options.http_req_concurrency,
            },
            retry: RetryConfig {
                num_tries: Some(3),
                secs_between_tries: 3,
            },
            block_window_size: options.block_window_size,
        }
    }
}
