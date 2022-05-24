use serde::Deserialize;
use std::path::PathBuf;

#[derive(Deserialize)]
pub struct Config {
    pub eth_rpc_url: url::Url,
    pub data_path: PathBuf,
    pub database_path: PathBuf,
    pub start_block: usize,
    pub end_block: usize,
    pub block: BlockConfig,
    pub log: LogConfig,
}

#[derive(Deserialize)]
pub struct BlockConfig {
    pub block_write_threshold: usize,
    pub tx_write_threshold: usize,
    pub batch_size: usize,
    pub concurrency: usize,
}

#[derive(Deserialize)]
pub struct LogConfig {
    pub log_write_threshold: usize,
    pub batch_size: usize,
    pub concurrency: usize,
}
