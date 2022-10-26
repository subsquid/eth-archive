use serde::Deserialize;
use std::path::PathBuf;

#[derive(Deserialize, Clone)]
pub struct ParquetConfig {
    pub name: String,
    pub items_per_file: usize,
    pub items_per_row_group: usize,
    pub path: PathBuf,
    pub channel_size: usize,
}
