use crate::{Error, Result};
use arrow2::array::{self, UInt32Array};
use arrow2::compute::concatenate::concatenate;
use arrow2::datatypes::{DataType, Field};
use arrow2::io::parquet;
use eth_archive_core::define_cols;
use eth_archive_core::dir_name::DirName;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::Path;
use std::{cmp, fs, io};
use xorf::{BinaryFuse16, BinaryFuse8};

type BinaryArray = array::BinaryArray<i32>;

#[derive(Serialize, Deserialize)]
pub struct ParquetMetadata {
    pub log: Vec<LogRowGroupMetadata>,
    pub tx: Vec<TransactionRowGroupMetadata>,
    pub block: Vec<BlockRowGroupMetadata>,
}

#[derive(Serialize, Deserialize)]
pub struct LogRowGroupMetadata {
    pub address_filter: BinaryFuse16,
    pub topic0_filter: BinaryFuse16,
}

#[derive(Serialize, Deserialize)]
pub struct TransactionRowGroupMetadata {
    pub source_filter: BinaryFuse16,
    pub dest_filter: BinaryFuse16,
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

    fn collect_log_meta(
        &self,
        addrs_global: &mut HashSet<u64>,
    ) -> Result<Vec<LogRowGroupMetadata>> {
        let mut path = self.data_path.to_owned();
        path.push(self.dir_name.to_string());
        path.push("log.parquet");
        let mut file = io::BufReader::new(fs::File::open(&path).map_err(Error::OpenParquetFile)?);

        let metadata = parquet::read::read_metadata(&mut file).map_err(Error::ReadParquet)?;

        let mut log_rg_meta = Vec::new();

        for row_group_meta in metadata.row_groups.iter() {
            let columns = parquet::read::read_columns_many(
                &mut file,
                row_group_meta,
                vec![
                    Field::new("address", DataType::Binary, false),
                    Field::new("topic0", DataType::Binary, true),
                ],
                None,
                None,
                None,
            )
            .map_err(Error::ReadParquet)?;

            let mut addrs = HashSet::new();
            let mut topic0_set = HashSet::new();

            #[rustfmt::skip]
            define_cols!(
                columns,
                address, BinaryArray,
                topic0, BinaryArray
            );

            let len = address.len();

            for i in 0..len {
                let address = address.get(i).unwrap();
                let addr_hash = hash(address);
                addrs.insert(addr_hash);
                addrs_global.insert(addr_hash);

                if let Some(topic) = topic0.get(i) {
                    topic0_set.insert(hash(topic));
                }
            }

            log_rg_meta.push(LogRowGroupMetadata {
                address_filter: BinaryFuse16::try_from(&addrs.into_iter().collect::<Vec<_>>())
                    .unwrap(),
                topic0_filter: BinaryFuse16::try_from(&topic0_set.into_iter().collect::<Vec<_>>())
                    .unwrap(),
            });
        }

        Ok(log_rg_meta)
    }

    fn collect_tx_meta(
        &self,
        addrs_global: &mut HashSet<u64>,
    ) -> Result<Vec<TransactionRowGroupMetadata>> {
        let mut path = self.data_path.to_owned();
        path.push(self.dir_name.to_string());
        path.push("tx.parquet");
        let mut file = io::BufReader::new(fs::File::open(&path).map_err(Error::OpenParquetFile)?);

        let metadata = parquet::read::read_metadata(&mut file).map_err(Error::ReadParquet)?;

        let mut tx_rg_meta = Vec::new();

        for row_group_meta in metadata.row_groups.iter() {
            let columns = parquet::read::read_columns_many(
                &mut file,
                row_group_meta,
                vec![
                    Field::new("source", DataType::Binary, true),
                    Field::new("dest", DataType::Binary, true),
                    Field::new("block_number", DataType::UInt32, false),
                    Field::new("transaction_index", DataType::UInt32, false),
                ],
                None,
                None,
                None,
            )
            .map_err(Error::ReadParquet)?;

            let mut max_blk_num_tx_idx = 0;
            let mut min_blk_num_tx_idx = 0;
            let mut source_addrs = HashSet::new();
            let mut dest_addrs = HashSet::new();

            #[rustfmt::skip]
            define_cols!(
                columns,
                source, BinaryArray,
                dest, BinaryArray,
                block_number, UInt32Array,
                transaction_index, UInt32Array
            );

            let len = block_number.len();

            for i in 0..len {
                if let Some(source) = source.get(i) {
                    let h = hash(source);
                    source_addrs.insert(h);
                    addrs_global.insert(h);
                }
                if let Some(dest) = dest.get(i) {
                    let h = hash(dest);
                    dest_addrs.insert(h);
                    addrs_global.insert(h);
                }
                let blk_num_tx_idx = combine_block_num_tx_idx(
                    block_number.get(i).unwrap(),
                    transaction_index.get(i).unwrap(),
                );

                max_blk_num_tx_idx = cmp::max(max_blk_num_tx_idx, blk_num_tx_idx);
                min_blk_num_tx_idx = cmp::min(min_blk_num_tx_idx, blk_num_tx_idx);
            }

            tx_rg_meta.push(TransactionRowGroupMetadata {
                source_filter: BinaryFuse16::try_from(
                    &source_addrs.into_iter().collect::<Vec<_>>(),
                )
                .unwrap(),
                dest_filter: BinaryFuse16::try_from(&dest_addrs.into_iter().collect::<Vec<_>>())
                    .unwrap(),
                max_blk_num_tx_idx,
                min_blk_num_tx_idx,
            });
        }

        Ok(tx_rg_meta)
    }

    fn collect_block_meta(&self) -> Result<Vec<BlockRowGroupMetadata>> {
        let mut path = self.data_path.to_owned();
        path.push(self.dir_name.to_string());
        path.push("block.parquet");
        let mut file = io::BufReader::new(fs::File::open(&path).map_err(Error::OpenParquetFile)?);

        let metadata = parquet::read::read_metadata(&mut file).map_err(Error::ReadParquet)?;

        let mut block_rg_meta = Vec::new();

        for row_group_meta in metadata.row_groups.iter() {
            let columns = parquet::read::read_columns_many(
                &mut file,
                row_group_meta,
                vec![Field::new("number", DataType::UInt32, false)],
                None,
                None,
                None,
            )
            .map_err(Error::ReadParquet)?;

            let mut max_block_number = 0;
            let mut min_block_number = 0;

            #[rustfmt::skip]
            define_cols!(
                columns,
                number, UInt32Array
            );

            let len = number.len();

            for i in 0..len {
                let blk_num = number.get(i).unwrap();
                max_block_number = cmp::max(max_block_number, blk_num);
                min_block_number = cmp::min(min_block_number, blk_num);
            }

            block_rg_meta.push(BlockRowGroupMetadata {
                max_block_number,
                min_block_number,
            });
        }

        Ok(block_rg_meta)
    }
}

pub fn hash(input: &[u8]) -> u64 {
    xxhash_rust::xxh3::xxh3_64(input)
}

pub fn combine_block_num_tx_idx(block_num: u32, tx_idx: u32) -> u64 {
    (u64::from(block_num) << 4) | u64::from(tx_idx)
}
