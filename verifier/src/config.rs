use clap::Parser;
use eth_archive_core::config::{IngestConfig, RetryConfig};
use std::num::NonZeroU32;
use url::Url;

#[derive(Clone, Parser, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct Config {
    #[command(flatten)]
    pub ingest: IngestConfig,
    #[command(flatten)]
    pub retry: RetryConfig,
    #[clap(long)]
    pub archive_url: Url,
    #[clap(long)]
    pub offsets: Vec<u32>,
    #[clap(long)]
    pub step: NonZeroU32,
    #[clap(long)]
    pub batches_per_step: usize,
}

impl Config {
    pub fn parse() -> Self {
        <Self as Parser>::parse()
    }
}
