use crate::Options;
use eth_archive_core::config::{DbConfig, IngestConfig, RetryConfig};
use parquet_writer::ParquetConfig;
use serde::Deserialize;

#[derive(Deserialize)]
pub struct Config {
    pub block: ParquetConfig,
    pub transaction: ParquetConfig,
    pub log: ParquetConfig,
    pub ingest: IngestConfig,
    pub retry: RetryConfig,
    pub db: DbConfig,
}

impl From<Options> for Config {
    fn from(options: Options) -> Self {
        Config {
            block: ParquetConfig {
                name: "block".to_string(),
                path: options.data_path.join("block"),
                items_per_file: 32768,
                items_per_row_group: 512,
                channel_size: 1024,
            },
            transaction: ParquetConfig {
                name: "tx".to_string(),
                path: options.data_path.join("tx"),
                items_per_file: 65536,
                items_per_row_group: 512,
                channel_size: 3072,
            },
            log: ParquetConfig {
                name: "log".to_string(),
                path: options.data_path.join("log"),
                items_per_file: 65536,
                items_per_row_group: 512,
                channel_size: 2048,
            },
            ingest: IngestConfig {
                eth_rpc_url: options.eth_rpc_url,
                block_batch_size: options.block_batch_size,
                http_req_concurrency: options.http_req_concurrency,
            },
            retry: RetryConfig {
                num_tries: None,
                secs_between_tries: 3,
            },
            db: DbConfig {
                user: options.db_user,
                password: options.db_password,
                host: options.db_host,
                port: options.db_port,
                dbname: options.db_name,
            },
        }
    }
}
