use clap::Parser;
use eth_archive_core::config::{DbConfig, IngestConfig, RetryConfig};

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct Config {
    #[command(flatten)]
    pub db: DbConfig,

    #[command(flatten)]
    pub ingest: IngestConfig,

    #[command(flatten)]
    pub retry: RetryConfig,

    /// Block window size
    #[clap(long)]
    pub block_window_size: usize,

    /// Delete indexed data from a database
    #[clap(short, long)]
    pub reset_data: bool,
}

impl Config {
    pub fn parse() -> Self {
        <Self as Parser>::parse()
    }
}
