#[derive(Deserialize)]
pub struct BlockConfig {
    pub block_write_threshold: usize,
    pub block_row_group_size: usize,
    pub tx_write_threshold: usize,
    pub tx_row_group_size: usize,
    pub batch_size: usize,
    pub concurrency: usize,
}

#[derive(Deserialize)]
pub struct LogConfig {
    pub log_write_threshold: usize,
    pub log_row_group_size: usize,
    pub batch_size: usize,
    pub concurrency: usize,
}

#[derive(Deserialize)]
pub struct RetryConfig {
    pub num_tries: usize,
    pub secs_between_tries: u64,
}