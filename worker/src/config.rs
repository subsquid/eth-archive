use clap::Parser;
use eth_archive_core::config::{IngestConfig, RetryConfig, S3Config};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::num::NonZeroUsize;
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
    /// Initial hot block range. If None, hot blocks will start from 0
    #[clap(long)]
    pub initial_hot_block_range: Option<u32>,
    /// Query stops as soon as the response body size in megabytes reaches this number.
    /// Response body might be bigger than this amount of MBs.
    #[clap(long)]
    pub max_resp_body_size: usize,
    #[clap(long, default_value_t = NonZeroUsize::new(1).unwrap())]
    pub query_concurrency: NonZeroUsize,
    /// Response time limit in milliseconds.
    /// The query will stop and found data will be returned
    /// if the request takes more than this amount of time to handle.
    #[clap(long)]
    pub resp_time_limit: u128,
    /// Size of each database query.
    /// Database queries are batched because we don't want to query the entire db at once.
    #[clap(long, default_value_t = 200)]
    pub db_query_batch_size: u32,
    #[command(flatten)]
    pub s3: S3Config,
    #[clap(long, default_value_t = 1800)]
    /// Period in seconds to run manual compaction on the database
    pub db_compaction_interval: u64,
}

fn default_server_addr() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080)
}

impl Config {
    pub fn parse() -> Self {
        <Self as Parser>::parse()
    }
}
