use crate::bloom::Bloom;
use crate::parquet_metadata::ParquetMetadata;
use crate::types::{LogQueryResult, MiniQuery, QueryResult};
use crate::{Error, Result};
use eth_archive_core::deserialize::Address;
use eth_archive_core::dir_name::DirName;
use eth_archive_core::ingest_metrics::IngestMetrics;
use eth_archive_core::types::{
    Block, BlockRange, Log, ResponseBlock, ResponseTransaction, Transaction,
};
use std::collections::{BTreeMap, BTreeSet};
use std::convert::TryInto;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Instant;
use std::{cmp, iter};
use tokio::sync::mpsc;

type ParquetIdxIter<'a> = Box<dyn Iterator<Item = Result<(DirName, Bloom<Address>)>> + Send + 'a>;

pub struct DbHandle {
    inner: rocksdb::DB,
    status: Status,
    metrics: Arc<IngestMetrics>,
}

struct Status {
    parquet_height: AtomicU32,
    db_height: AtomicU32,
    db_tail: AtomicU32,
}

impl DbHandle {
    pub async fn new(path: &Path, metrics: Arc<IngestMetrics>) -> Result<DbHandle> {
        let path = path.to_owned();
        tokio::task::spawn_blocking(move || Self::new_impl(path, metrics))
            .await
            .unwrap()
    }

    fn new_impl(path: PathBuf, metrics: Arc<IngestMetrics>) -> Result<DbHandle> {
        let mut block_opts = rocksdb::BlockBasedOptions::default();

        block_opts.set_block_size(1024 * 1024);
        block_opts.set_format_version(5);
        block_opts.set_bloom_filter(10.0, true);

        let mut opts = rocksdb::Options::default();

        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
        opts.set_bytes_per_sync(1048576);
        opts.set_max_open_files(10000);
        opts.set_block_based_table_factory(&block_opts);
        opts.set_compaction_style(rocksdb::DBCompactionStyle::Level);
        opts.set_level_compaction_dynamic_level_bytes(true);

        let inner =
            rocksdb::DB::open_cf(&opts, path, cf_name::ALL_CF_NAMES).map_err(Error::OpenDb)?;

        let status = Self::get_status(&inner)?;

        Ok(Self {
            inner,
            metrics,
            status,
        })
    }

    pub async fn get_parquet_metadata(
        self: Arc<Self>,
        dir_name: DirName,
    ) -> Result<Option<ParquetMetadata>> {
        tokio::task::spawn_blocking(move || self.get_parquet_metadata_impl(dir_name))
            .await
            .unwrap()
    }

    fn get_parquet_metadata_impl(&self, dir_name: DirName) -> Result<Option<ParquetMetadata>> {
        let parquet_metadata_cf = self.inner.cf_handle(cf_name::PARQUET_METADATA).unwrap();

        let metadata = self
            .inner
            .get_cf(parquet_metadata_cf, key_from_dir_name(dir_name))
            .map_err(Error::Db)?;

        let metadata = metadata
            .as_ref()
            .map(|m| rmp_serde::decode::from_slice(m).unwrap());

        Ok(metadata)
    }

    pub async fn iter_parquet_idxs(
        self: Arc<Self>,
        from: u32,
        to: Option<u32>,
    ) -> mpsc::Receiver<Result<(DirName, Bloom<Address>)>> {
        let (tx, rx): (_, _) = mpsc::channel(1);

        tokio::task::spawn_blocking(move || {
            let iter = self.iter_parquet_idxs_impl(from, to).unwrap();
            for res in iter {
                if tx.blocking_send(res).is_err() {
                    break;
                }
            }
        });

        rx
    }

    fn iter_parquet_idxs_impl(&self, from: u32, to: Option<u32>) -> Result<ParquetIdxIter<'_>> {
        let parquet_idx_cf = self.inner.cf_handle(cf_name::PARQUET_IDX).unwrap();

        let key = key_from_dir_name(DirName {
            range: BlockRange {
                from,
                to: std::u32::MAX,
            },
            is_temp: false,
        });

        let mut iter = self.inner.iterator_cf(
            parquet_idx_cf,
            rocksdb::IteratorMode::From(&key, rocksdb::Direction::Reverse),
        );

        let start_key = match iter.next() {
            Some(Ok((start_key, _))) => start_key,
            Some(Err(e)) => return Err(Error::Db(e)),
            None => return Ok(Box::new(iter::empty())),
        };

        let iter = self
            .inner
            .iterator_cf(
                parquet_idx_cf,
                rocksdb::IteratorMode::From(&start_key, rocksdb::Direction::Forward),
            )
            .map(|idx| {
                let (dir_name, idx) = idx.map_err(Error::Db)?;
                let dir_name = dir_name_from_key(&dir_name);
                let idx = rmp_serde::decode::from_slice(&idx).unwrap();

                Ok((dir_name, idx))
            })
            .take_while(move |res| {
                let (dir_name, _) = match res {
                    Ok(ref a) => a,
                    Err(_) => return true,
                };

                match to {
                    Some(to) => dir_name.range.from < to,
                    None => true,
                }
            });

        Ok(Box::new(iter))
    }

    pub async fn query(self: Arc<Self>, query: MiniQuery) -> Result<QueryResult> {
        tokio::task::spawn_blocking(move || self.query_impl(query))
            .await
            .unwrap()
    }

    fn query_impl(&self, query: MiniQuery) -> Result<QueryResult> {
        let LogQueryResult {
            logs,
            transactions,
            blocks,
        } = if !query.logs.is_empty() {
            self.query_logs(&query)?
        } else {
            LogQueryResult::default()
        };

        let mut blocks = blocks;

        let transactions = if query.transactions.is_empty() && transactions.is_empty() {
            BTreeMap::new()
        } else {
            self.query_transactions(&query, &transactions, &mut blocks)?
        };

        let blocks = if query.include_all_blocks {
            None
        } else {
            Some(&blocks)
        };

        let blocks = self.query_blocks(&query, blocks)?;

        Ok(QueryResult {
            logs,
            transactions,
            blocks,
        })
    }

    fn query_logs(&self, query: &MiniQuery) -> Result<LogQueryResult> {
        let log_cf = self.inner.cf_handle(cf_name::LOG).unwrap();

        let mut query_result = LogQueryResult {
            logs: BTreeMap::new(),
            transactions: BTreeSet::new(),
            blocks: BTreeSet::new(),
        };

        for res in self.inner.iterator_cf(
            log_cf,
            rocksdb::IteratorMode::From(
                &query.from_block.to_be_bytes(),
                rocksdb::Direction::Forward,
            ),
        ) {
            let (log_key, log) = res.map_err(Error::Db)?;

            if log_key.as_ref() >= query.to_block.to_be_bytes().as_slice() {
                break;
            }

            let log: Log = rmp_serde::decode::from_slice(&log).unwrap();

            if !query.matches_log(&log.address, &log.topics) {
                continue;
            }

            query_result
                .transactions
                .insert((log.block_number.0, log.transaction_index.0));
            query_result.blocks.insert(log.block_number.0);
            query_result.logs.insert(
                (log.block_number.0, log.log_index.0),
                query.field_selection.log.prune(log),
            );
        }

        Ok(query_result)
    }

    fn query_transactions(
        &self,
        query: &MiniQuery,
        transactions: &BTreeSet<(u32, u32)>,
        blocks: &mut BTreeSet<u32>,
    ) -> Result<BTreeMap<(u32, u32), ResponseTransaction>> {
        let tx_cf = self.inner.cf_handle(cf_name::TX).unwrap();

        let mut res_transactions = BTreeMap::new();

        for res in self.inner.iterator_cf(
            tx_cf,
            rocksdb::IteratorMode::From(
                &query.from_block.to_be_bytes(),
                rocksdb::Direction::Forward,
            ),
        ) {
            let (tx_key, tx) = res.map_err(Error::Db)?;

            if tx_key.as_ref() >= query.to_block.to_be_bytes().as_slice() {
                break;
            }

            let tx: Transaction = rmp_serde::decode::from_slice(&tx).unwrap();

            let tx_id = (tx.block_number.0, tx.transaction_index.0);

            if transactions.contains(&tx_id) || !query.matches_tx(&tx) {
                continue;
            }

            blocks.insert(tx.block_number.0);
            res_transactions.insert(tx_id, query.field_selection.transaction.prune(tx));
        }

        Ok(res_transactions)
    }

    fn query_blocks(
        &self,
        query: &MiniQuery,
        blocks: Option<&BTreeSet<u32>>,
    ) -> Result<BTreeMap<u32, ResponseBlock>> {
        let block_cf = self.inner.cf_handle(cf_name::BLOCK).unwrap();

        let mut res_blocks = BTreeMap::new();

        for res in self.inner.iterator_cf(
            block_cf,
            rocksdb::IteratorMode::From(
                &query.from_block.to_be_bytes(),
                rocksdb::Direction::Forward,
            ),
        ) {
            let (block_key, block) = res.map_err(Error::Db)?;

            if block_key.as_ref() >= query.to_block.to_be_bytes().as_slice() {
                break;
            }

            let block: Block = rmp_serde::decode::from_slice(&block).unwrap();

            if let Some(blocks) = blocks {
                if blocks.contains(&block.number.0) {
                    res_blocks.insert(block.number.0, query.field_selection.block.prune(block));
                }
            } else {
                res_blocks.insert(block.number.0, query.field_selection.block.prune(block));
            }
        }

        Ok(res_blocks)
    }

    pub fn register_parquet_folder(
        &self,
        dir_name: DirName,
        idx: &Bloom<Address>,
        metadata: &ParquetMetadata,
    ) -> Result<()> {
        let parquet_idx_cf = self.inner.cf_handle(cf_name::PARQUET_IDX).unwrap();
        let parquet_metadata_cf = self.inner.cf_handle(cf_name::PARQUET_METADATA).unwrap();

        let key = key_from_dir_name(dir_name);

        let idx_val = rmp_serde::encode::to_vec(idx).unwrap();
        let metadata_val = rmp_serde::encode::to_vec(metadata).unwrap();

        let mut batch = rocksdb::WriteBatch::default();

        batch.put_cf(parquet_idx_cf, key, idx_val);
        batch.put_cf(parquet_metadata_cf, key, metadata_val);

        let mut db_tail = self.status.db_tail.load(Ordering::Relaxed);

        for cf in [cf_name::BLOCK, cf_name::TX, cf_name::LOG] {
            let cf = self.inner.cf_handle(cf).unwrap();

            for res in self.inner.iterator_cf(
                cf,
                rocksdb::IteratorMode::From(&0u32.to_be_bytes(), rocksdb::Direction::Forward),
            ) {
                let (key, _) = res.map_err(Error::Db)?;

                if key.as_ref() >= dir_name.range.to.to_be_bytes().as_slice() {
                    break;
                }

                batch.delete_cf(cf, &key);

                db_tail = cmp::max(db_tail, block_num_from_key(&key));
            }
        }

        self.inner.write(batch).map_err(Error::Db)?;

        self.status
            .parquet_height
            .store(dir_name.range.to, Ordering::Relaxed);
        self.status.db_tail.store(db_tail, Ordering::Relaxed);
        let height = self.height();
        if height > 0 {
            self.metrics.record_write_height(height - 1);
        }

        Ok(())
    }

    pub fn insert_batches(
        &self,
        (block_ranges, block_batches, log_batches): (
            Vec<BlockRange>,
            Vec<Vec<Block>>,
            Vec<Vec<Log>>,
        ),
    ) -> Result<()> {
        let block_cf = self.inner.cf_handle(cf_name::BLOCK).unwrap();
        let tx_cf = self.inner.cf_handle(cf_name::TX).unwrap();
        let log_cf = self.inner.cf_handle(cf_name::LOG).unwrap();

        let mut batch = rocksdb::WriteBatch::default();

        let mut db_height = self.status.db_height.load(Ordering::Relaxed);

        for (_, (blocks, logs)) in block_ranges
            .into_iter()
            .zip(block_batches.into_iter().zip(log_batches.into_iter()))
        {
            for block in blocks.iter() {
                let val = rmp_serde::encode::to_vec(block).unwrap();
                batch.put_cf(block_cf, block.number.to_be_bytes(), &val);

                for tx in block.transactions.iter() {
                    let val = rmp_serde::encode::to_vec(tx).unwrap();
                    let tx_key = tx_key(tx);

                    batch.put_cf(tx_cf, tx_key, &val);
                }

                db_height = cmp::max(db_height, block.number.0 + 1);
            }

            for log in logs {
                let val = rmp_serde::encode::to_vec(&log).unwrap();
                let log_key = log_key(&log);
                batch.put_cf(log_cf, log_key, &val);
            }
        }

        self.inner.write(batch).map_err(Error::Db)?;

        let db_tail = self
            .inner
            .iterator_cf(block_cf, rocksdb::IteratorMode::Start)
            .next()
            .transpose()
            .map_err(Error::Db)?
            .map(|(key, _)| block_num_from_key(&key))
            .unwrap_or(0);

        self.status.db_tail.store(db_tail, Ordering::Relaxed);
        self.status.db_height.store(db_height, Ordering::Relaxed);
        let height = self.height();
        if height > 0 {
            self.metrics.record_write_height(height - 1);
        }

        Ok(())
    }

    pub fn height(&self) -> u32 {
        let parquet_height = self.status.parquet_height.load(Ordering::Relaxed);
        let db_height = self.status.db_height.load(Ordering::Relaxed);
        let db_tail = self.status.db_tail.load(Ordering::Relaxed);

        if db_tail <= parquet_height {
            db_height
        } else {
            parquet_height
        }
    }

    pub fn parquet_height(&self) -> u32 {
        self.status.parquet_height.load(Ordering::Relaxed)
    }

    pub fn db_height(&self) -> u32 {
        self.status.db_height.load(Ordering::Relaxed)
    }

    fn get_status(inner: &rocksdb::DB) -> Result<Status> {
        let parquet_idx_cf = inner.cf_handle(cf_name::PARQUET_IDX).unwrap();

        let parquet_height = inner
            .iterator_cf(parquet_idx_cf, rocksdb::IteratorMode::End)
            .next()
            .transpose()
            .map_err(Error::Db)?
            .map(|(key, _)| dir_name_from_key(&key).range.to)
            .unwrap_or(0);

        let block_cf = inner.cf_handle(cf_name::BLOCK).unwrap();

        let db_tail = inner
            .iterator_cf(block_cf, rocksdb::IteratorMode::Start)
            .next()
            .transpose()
            .map_err(Error::Db)?
            .map(|(key, _)| block_num_from_key(&key))
            .unwrap_or(0);

        let db_height = inner
            .iterator_cf(block_cf, rocksdb::IteratorMode::End)
            .next()
            .transpose()
            .map_err(Error::Db)?
            .map(|(key, _)| block_num_from_key(&key) + 1)
            .unwrap_or(0);

        Ok(Status {
            parquet_height: AtomicU32::new(parquet_height),
            db_tail: AtomicU32::new(db_tail),
            db_height: AtomicU32::new(db_height),
        })
    }

    pub fn compact(&self) {
        let start = Instant::now();

        log::info!("starting compaction...");

        let compact = |name| {
            self.inner.compact_range_cf(
                self.inner.cf_handle(name).unwrap(),
                None::<&[u8]>,
                None::<&[u8]>,
            );
        };

        for cf in cf_name::ALL_CF_NAMES.iter() {
            compact(cf);
        }

        log::info!("finished compaction in {}ms", start.elapsed().as_millis());
    }
}

/// Column Family Names
mod cf_name {
    pub const BLOCK: &str = "BLOCK";
    pub const TX: &str = "TX";
    pub const LOG: &str = "LOG";
    pub const PARQUET_IDX: &str = "PARQUET_IDX";
    pub const PARQUET_METADATA: &str = "PARQUET_METADATA";

    pub const ALL_CF_NAMES: [&str; 5] = [BLOCK, TX, LOG, PARQUET_IDX, PARQUET_METADATA];
}

fn tx_key(tx: &Transaction) -> [u8; 8] {
    let mut key = [0; 8];

    key[..4].copy_from_slice(&tx.block_number.0.to_be_bytes());
    key[4..].copy_from_slice(&tx.transaction_index.0.to_be_bytes());

    key
}

fn log_key(log: &Log) -> [u8; 8] {
    let mut key = [0; 8];

    key[..4].copy_from_slice(&log.block_number.to_be_bytes());
    key[4..].copy_from_slice(&log.log_index.to_be_bytes());

    key
}

fn dir_name_from_key(key: &[u8]) -> DirName {
    let from = (&key[..4]).try_into().unwrap();
    let from = u32::from_be_bytes(from);

    let to = (&key[4..]).try_into().unwrap();
    let to = u32::from_be_bytes(to);

    DirName {
        range: BlockRange { from, to },
        is_temp: false,
    }
}

fn key_from_dir_name(dir_name: DirName) -> [u8; 8] {
    assert!(!dir_name.is_temp);

    let mut key = [0; 8];

    key[..4].copy_from_slice(&dir_name.range.from.to_be_bytes());
    key[4..].copy_from_slice(&dir_name.range.to.to_be_bytes());

    key
}

fn block_num_from_key(key: &[u8]) -> u32 {
    let arr: [u8; 4] = key[..4].try_into().unwrap();

    u32::from_be_bytes(arr)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dir_name_key_roundtrip() {
        let dir_name = DirName {
            range: BlockRange {
                from: 12345,
                to: 123456,
            },
            is_temp: false,
        };

        let key = key_from_dir_name(dir_name);

        assert_eq!(dir_name, dir_name_from_key(&key));
    }

    #[test]
    fn test_dir_name_key_ordering() {
        let dir_name0 = DirName {
            range: BlockRange {
                from: 12345,
                to: 123456,
            },
            is_temp: false,
        };
        let dir_name1 = DirName {
            range: BlockRange {
                from: 123456,
                to: 1234567,
            },
            is_temp: false,
        };

        let key0 = key_from_dir_name(dir_name0);
        let key1 = key_from_dir_name(dir_name1);

        assert!(key0 < key1);
    }
}
