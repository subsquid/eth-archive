use serde::Deserialize;
use std::path::PathBuf;

#[derive(Deserialize)]
pub struct Config {
    pub eth_rpc_url: url::Url,
    pub parquet_path: PathBuf,
    pub database_path: PathBuf,
    pub start_block: u64,
    pub end_block: u64,
    pub block_cfg: DigestConfig,
    pub log_cfg: DigestConfig,
}

#[derive(Deserialize)]
pub struct DigestConfig {
    pub parquet_write_threshold: usize,
    pub batch_size: usize,
    pub concurrency: usize,
}
