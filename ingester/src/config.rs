use clap::Parser;
use eth_archive_core::config::{IngestConfig, RetryConfig};
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
    /// Maximum number of blocks per parquet file
    #[clap(long)]
    pub max_blocks_per_file: usize,
    /// Maximum number of transactions per file
    #[clap(long)]
    pub max_txs_per_file: usize,
    /// Maximum number of logs per parquet file
    #[clap(long)]
    pub max_logs_per_file: usize,
}

impl Config {
    pub fn parse() -> Self {
        <Self as Parser>::parse()
    }
}
