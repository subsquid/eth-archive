use clap::Parser;
use eth_archive_core::config::{IngestConfig, RetryConfig};

#[derive(Clone, Parser, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct Config {
    #[command(flatten)]
    pub ingest: IngestConfig,
    #[command(flatten)]
    pub retry: RetryConfig,
}

impl Config {
    pub fn parse() -> Self {
        <Self as Parser>::parse()
    }
}
