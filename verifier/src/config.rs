use clap::Parser;
use eth_archive_core::config::{IngestConfig, RetryConfig, S3Config};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;

#[derive(Clone, Parser, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct Config {
    #[command(flatten)]
    pub ingest: IngestConfig,
    #[command(flatten)]
    pub retry: RetryConfig,
    /// Address to serve prometheus metrics from
    #[clap(long, default_value_t = default_metrics_addr())]
    pub metrics_addr: SocketAddr,
    /// Url of worker to verify.
    #[clap(long)]
    pub worker_url: url::Url,
    /// Number of blocks to jump for each test
    /// This is only effective when catching up to worker height
    #[clap(long)]
    pub step: u32,
    /// This verification is for checking unexpectedly deep rollbacks
    #[clap(long)]
    pub short_trail_distance: u32,
    /// This verification is for checking parquet queries
    #[clap(long)]
    pub long_trail_distance: u32,
}

fn default_metrics_addr() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8181)
}

impl Config {
    pub fn parse() -> Self {
        <Self as Parser>::parse()
    }
}
