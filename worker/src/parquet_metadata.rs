use crate::db::ParquetIdx;
use crate::{Error, Result};
use eth_archive_core::dir_name::DirName;
use serde::{Deserialize, Serialize};
use std::path::Path;
use xorf::BinaryFuse16;

#[derive(Serialize, Deserialize)]
pub struct ParquetMetadata {
    pub log: Vec<LogRowGroupMetadata>,
    pub tx: Vec<TransactionRowGroupMetadata>,
    pub block: Vec<BlockRowGroupMetadata>,
}

#[derive(Serialize, Deserialize)]
pub struct LogRowGroupMetadata {
    pub address_filter: BinaryFuse16,
    pub address_topic0_filter: BinaryFuse16,
}

#[derive(Serialize, Deserialize)]
pub struct TransactionRowGroupMetadata {
    pub from_addr_filter: BinaryFuse16,
    pub to_addr_filter: BinaryFuse16,
    pub max_blk_num_tx_idx: u64,
    pub min_blk_num_tx_idx: u64,
}

#[derive(Serialize, Deserialize)]
pub struct BlockRowGroupMetadata {
    pub max_block_number: u32,
    pub min_block_number: u32,
}

pub struct CollectMetadataAndParquetIdx<'a> {
    pub data_path: &'a Path,
    pub dir_name: DirName,
}

impl<'a> CollectMetadataAndParquetIdx<'a> {
    pub fn collect(self) -> Result<(ParquetMetadata, ParquetIdx)> {
        todo!()
    }
}
