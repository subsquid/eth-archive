use clap::Parser;
use eth_archive_core::config::{DbConfig, RetryConfig};
use std::net::Ipv4Addr;
use std::path::PathBuf;

#[derive(Parser, Debug)]
pub struct Config {
    #[clap(flatten)]
    pub db: DbConfig,

    #[clap(flatten)]
    pub retry: RetryConfig,

    #[clap(flatten)]
    pub data: DataConfig,

    /// Ip to be used for running server
    #[clap(long, default_value_t = Ipv4Addr::new(127, 0, 0, 1))]
    pub http_server_ip: Ipv4Addr,

    /// Port to be used for running server
    #[clap(long, default_value_t = 8080)]
    pub http_server_port: u16,
}

impl Config {
    pub fn parse() -> Self {
        <Self as Parser>::parse()
    }
}

#[derive(Parser, Clone, Debug)]
pub struct DataConfig {
    /// A path to indexed parquet files
    #[clap(long)]
    pub data_path: PathBuf,

    /// Max block range
    #[clap(long)]
    pub max_block_range: u32,

    /// Default block range
    #[clap(long)]
    pub default_block_range: u32,

    /// Response log limit
    #[clap(long)]
    pub response_log_limit: usize,

    /// Query chunk size
    #[clap(long)]
    pub query_chunk_size: u32,

    /// Time limit to execute query
    #[clap(long)]
    pub query_time_limit_ms: u64,
}
