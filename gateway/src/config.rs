use crate::Options;
use eth_archive_core::config::{DbConfig, RetryConfig};
use serde::Deserialize;
use std::net::Ipv4Addr;
use std::path::PathBuf;

#[derive(Deserialize)]
pub struct Config {
    pub retry: RetryConfig,
    pub db: DbConfig,
    pub data: DataConfig,
    pub http_server: HttpServerConfig,
}

#[derive(Deserialize, Clone)]
pub struct DataConfig {
    pub blocks_path: PathBuf,
    pub transactions_path: PathBuf,
    pub logs_path: PathBuf,
    pub max_block_range: u32,
    pub default_block_range: u32,
    pub response_log_limit: usize,
    pub query_chunk_size: u32,
    pub query_time_limit_ms: u64,
}

#[derive(Deserialize)]
pub struct HttpServerConfig {
    pub ip: Ipv4Addr,
    pub port: u16,
}

impl From<Options> for Config {
    fn from(options: Options) -> Self {
        Config {
            retry: RetryConfig {
                num_tries: None,
                secs_between_tries: 3,
            },
            db: DbConfig {
                user: options.db_user,
                password: options.db_password,
                dbname: options.db_name,
                host: options.db_host,
                port: options.db_port,
            },
            data: DataConfig {
                blocks_path: options.data_path.join("block"),
                transactions_path: options.data_path.join("tx"),
                logs_path: options.data_path.join("log"),
                query_chunk_size: options.query_chunk_size,
                query_time_limit_ms: options.query_time_limit_ms,
            },
            http_server: HttpServerConfig {
                ip: options.ip,
                port: options.port,
            },
        }
    }
}
