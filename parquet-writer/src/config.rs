#[derive(Deserialize)]
pub struct BlockConfig {
    pub blocks_per_file: usize,
    pub blocks_per_row_group: usize,
    pub transactions_per_file: usize,
    pub transactions_per_row_group: usize,
}

#[derive(Deserialize)]
pub struct LogConfig {
    pub logs_per_file: usize,
    pub logs_per_row_group: usize,
}
