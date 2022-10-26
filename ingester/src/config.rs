use clap::Parser;
use eth_archive_core::config::{DbConfig, IngestConfig, RetryConfig};
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct Config {
    /// A path to store indexed parquet files
    #[clap(long)]
    pub data_path: PathBuf,

    #[command(flatten)]
    pub ingest: IngestConfig,

    #[command(flatten)]
    pub retry: RetryConfig,

    #[command(flatten)]
    pub db: DbConfig,

    /// Delete indexed parquet files
    #[clap(short, long)]
    pub reset_data: bool,
}

impl Config {
    pub fn parse() -> Self {
        <Self as Parser>::parse()
    }
}
