use clap::Parser;
use eth_archive_core::config::{FormatVersion, IngestConfig, RetryConfig, S3Config};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;

#[derive(Clone, Parser, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct Config {
    /// Path to store parquet files
    #[clap(long)]
    pub data_path: PathBuf,
    #[command(flatten)]
    pub ingest: IngestConfig,
    #[command(flatten)]
    pub retry: RetryConfig,
    /// Maximum number of blocks per parquet file
    #[clap(long)]
    pub max_blocks_per_file: usize,
    /// Maximum number of transactions per file
    #[clap(long)]
    pub max_txs_per_file: usize,
    /// Maximum number of logs per parquet file
    #[clap(long)]
    pub max_logs_per_file: usize,
    /// Maximum number of row groups per parquet file
    #[clap(long)]
    pub max_row_groups_per_file: usize,
    /// Page size for parquet files in bytes. Defaults to 1MB.
    #[clap(long)]
    pub parquet_page_size: Option<usize>,
    /// Maximum number of pending folder writes.
    /// This effects maximum memory consumption.
    #[clap(long, default_value_t = 8)]
    pub max_pending_folder_writes: usize,
    #[clap(long, default_value_t = 8)]
    pub folder_write_concurrency: usize,
    /// Address to serve prometheus metrics from
    #[clap(long, default_value_t = default_metrics_addr())]
    pub metrics_addr: SocketAddr,
    /// S3 bucket name to initial sync from
    #[clap(long)]
    pub s3_src_bucket: Option<String>,
    /// Source data format version
    #[clap(long)]
    pub s3_src_format_ver: Option<FormatVersion>,
    #[command(flatten)]
    pub s3: S3Config,
}

fn default_metrics_addr() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8181)
}

impl Config {
    pub fn parse() -> Self {
        <Self as Parser>::parse()
    }
}
