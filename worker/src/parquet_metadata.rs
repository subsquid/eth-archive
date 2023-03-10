use crate::{Error, Result};
use eth_archive_core::dir_name::DirName;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::Path;
use xorf::{BinaryFuse16, BinaryFuse8};

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
    pub fn collect(self) -> Result<(ParquetMetadata, BinaryFuse8)> {
        let mut addrs = HashSet::new();

        let log = self.collect_log_meta(&mut addrs)?;
        let tx = self.collect_tx_meta(&mut addrs)?;
        let block = self.collect_block_meta()?;

        let filter = BinaryFuse8::try_from(&addrs.into_iter().collect::<Vec<_>>()).unwrap();

        let metadata = ParquetMetadata { log, tx, block };

        Ok((metadata, filter))
    }

    fn collect_log_meta(&self, addrs: &mut HashSet<u64>) -> Result<Vec<LogRowGroupMetadata>> {
        todo!()
    }

    fn collect_tx_meta(
        &self,
        addrs: &mut HashSet<u64>,
    ) -> Result<Vec<TransactionRowGroupMetadata>> {
        todo!()
    }

    fn collect_block_meta(&self) -> Result<Vec<BlockRowGroupMetadata>> {
        todo!()
    }
}

fn hash(input: &[u8]) -> u64 {
    xxhash_rust::xxh3::xxh3_64(input)
}
