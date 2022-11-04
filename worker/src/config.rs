use clap::Parser;
use eth_archive_core::config::{IngestConfig, RetryConfig};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;

#[derive(Parser, Debug)]
pub struct Config {
    /// Database path
    #[clap(long)]
    pub db_path: PathBuf,
    /// Path to read parquet files from
    #[clap(long)]
    pub data_path: PathBuf,
    #[command(flatten)]
    pub ingest: IngestConfig,
    #[command(flatten)]
    pub retry: RetryConfig,
    /// Address to be used for running server
    #[clap(long, default_value_t = default_server_addr())]
    pub server_addr: SocketAddr,
    /// Minimum hot block range.
    /// Blocks that fall out of this range will be periodically deleted.
    #[command(flatten)]
    pub min_hot_block_range: u32,
    /// Maximum response body size
    #[clap(long)]
    pub max_resp_body_size: usize,
    /// Response time limit in milliseconds.
    /// The query will stop and found data will be returned
    /// if the request takes more than this amount of time to handle.
    #[clap(long)]
    pub response_time_limit_millis: u64,
}

const fn default_server_addr() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080)
}

impl Config {
    pub fn parse() -> Self {
        <Self as Parser>::parse()
    }
}
