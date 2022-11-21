use clap::Parser;
use eth_archive_core::config::{IngestConfig, RetryConfig, S3Config};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::num::NonZeroU32;

#[derive(Clone, Parser, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct Config {
    #[command(flatten)]
    pub ingest: IngestConfig,
    #[command(flatten)]
    pub retry: RetryConfig,
    /// Url of archive to verify.
    #[clap(long)]
    pub archive_url: url::Url,
    /// Number of blocks to jump for each test
    /// This is only effective when catching up to worker height
    #[clap(long)]
    pub step: NonZeroU32,
    /// This auxiliary verification is for checking unexpectedly deep rollbacks
    #[clap(long)]
    pub short_trail_distance: NonZeroU32,
    /// This auxiliary verification is for checking parquet queries
    #[clap(long)]
    pub long_trail_distance: NonZeroU32,
}

impl Config {
    pub fn parse() -> Self {
        <Self as Parser>::parse()
    }
}
