use crate::types::MiniQuery;
use crate::{Error, Result};
use eth_archive_core::deserialize::Address;
use eth_archive_core::dir_name::DirName;
use eth_archive_core::ingest_metrics::IngestMetrics;
use eth_archive_core::types::{
    Block, BlockRange, Log, QueryMetrics, QueryResult, ResponseRow, Transaction,
};
use serde::{Deserialize, Serialize};
use solana_bloom::bloom::Bloom as BloomFilter;
use std::collections::{BTreeMap, BTreeSet};
use std::convert::TryInto;
use std::path::Path;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Instant;
use std::{cmp, iter, mem};

pub type Bloom = BloomFilter<Address>;
pub type ParquetIdxIter<'a> = Box<dyn Iterator<Item = Result<(DirName, ParquetIdx)>> + 'a>;

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

        let (inner, status) = tokio::task::spawn_blocking(move || {
            let mut block_opts = rocksdb::BlockBasedOptions::default();

            block_opts.set_block_size(32 * 1024);
            block_opts.set_format_version(5);
            block_opts.set_ribbon_filter(10.0);

            let mut opts = rocksdb::Options::default();

            opts.create_if_missing(true);
            opts.create_missing_column_families(true);
            opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
            opts.set_bottommost_compression_type(rocksdb::DBCompressionType::Lz4);
            opts.set_level_compaction_dynamic_level_bytes(true);
            opts.set_max_background_jobs(6);
            opts.set_bytes_per_sync(1048576);
            opts.set_max_open_files(10000);
            opts.set_block_based_table_factory(&block_opts);

            let inner =
                rocksdb::DB::open_cf(&opts, path, cf_name::ALL_CF_NAMES).map_err(Error::OpenDb)?;

            let status = Self::get_status(&inner)?;

            Ok((inner, status))
        })
        .await
        .map_err(Error::TaskJoinError)??;

        Ok(Self {
            inner,
            status,
            metrics,
        })
    }

    pub fn iter_parquet_idxs(&self, from: u32, to: Option<u32>) -> Result<ParquetIdxIter<'_>> {
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

        let iterator = self
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
                let (dir_name, _) = match &res {
                    Ok(a) => a,
                    Err(_) => return true,
                };

                match to {
                    Some(to) => dir_name.range.from < to,
                    None => true,
                }
            });

        Ok(Box::new(iterator))
    }

    pub fn query(&self, query: MiniQuery) -> Result<QueryResult> {
        let mut metrics = QueryMetrics::default();
        let mut data = vec![];

        if !query.logs.is_empty() {
            let logs = self.query_logs(&query)?;
            metrics += logs.metrics;
            data.extend_from_slice(&logs.data);
        }

        if !query.transactions.is_empty() {
            let transactions = self.query_transactions(&query)?;
            metrics += transactions.metrics;
            data.extend_from_slice(&transactions.data);
        }

        Ok(QueryResult { data, metrics })
    }

    fn query_logs(&self, query: &MiniQuery) -> Result<QueryResult> {
        let block_cf = self.inner.cf_handle(cf_name::BLOCK).unwrap();
        let log_cf = self.inner.cf_handle(cf_name::LOG).unwrap();
        let log_tx_cf = self.inner.cf_handle(cf_name::LOG_TX).unwrap();

        let start = Instant::now();

        let mut block_nums = BTreeSet::new();
        let mut tx_keys = BTreeSet::new();
        let mut logs = BTreeMap::new();

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

            if !query.matches_log_addr(&log_key_to_address(&log_key)) {
                continue;
            }

            let log: Log = rmp_serde::decode::from_slice(&log).unwrap();

            if !query.matches_log(&log) {
                continue;
            }

            block_nums.insert(log.block_number.0);
            tx_keys.insert(log_tx_key(log.block_number.0, log.transaction_index.0));

            let log = query.field_selection.log.prune(log);
            logs.insert(log_key, log);
        }

        let mut blocks = BTreeMap::new();
        let mut txs = BTreeMap::new();

        for num in block_nums {
            let block = self
                .inner
                .get_pinned_cf(block_cf, num.to_be_bytes())
                .map_err(Error::Db)?
                .unwrap();
            let block = rmp_serde::decode::from_slice(&block).unwrap();

            let block = query.field_selection.block.prune(block);

            blocks.insert(num, block);
        }

        for key in tx_keys {
            let tx = self
                .inner
                .get_pinned_cf(log_tx_cf, key)
                .map_err(Error::Db)?
                .unwrap();
            let tx = rmp_serde::decode::from_slice(&tx).unwrap();

            let tx = query.field_selection.transaction.prune(tx);

            txs.insert(key, tx);
        }

        let data = logs
            .into_values()
            .map(|log| ResponseRow {
                block: blocks.get(&log.block_number.unwrap().0).unwrap().clone(),
                transaction: txs
                    .get(&log_tx_key(
                        log.block_number.unwrap().0,
                        log.transaction_index.unwrap().0,
                    ))
                    .unwrap()
                    .clone(),
                log: Some(log),
            })
            .collect();

        let elapsed = start.elapsed().as_millis();

        Ok(QueryResult {
            data,
            metrics: QueryMetrics {
                run_query: elapsed,
                total: elapsed,
                ..Default::default()
            },
        })
    }

    fn query_transactions(&self, query: &MiniQuery) -> Result<QueryResult> {
        let block_cf = self.inner.cf_handle(cf_name::BLOCK).unwrap();
        let tx_cf = self.inner.cf_handle(cf_name::TX).unwrap();

        let start = Instant::now();

        let mut block_nums = BTreeSet::new();
        let mut txs = BTreeMap::new();

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

            if !query.matches_tx_dest(&tx_key_to_dest(&tx_key)) {
                continue;
            }

            let tx: Transaction = rmp_serde::decode::from_slice(&tx).unwrap();

            if !query.matches_tx(&tx) {
                continue;
            }

            block_nums.insert(tx.block_number.0);

            let tx = query.field_selection.transaction.prune(tx);
            txs.insert(tx_key, tx);
        }

        let mut blocks = BTreeMap::new();

        for num in block_nums {
            let block = self
                .inner
                .get_pinned_cf(block_cf, num.to_be_bytes())
                .map_err(Error::Db)?
                .unwrap();
            let block = rmp_serde::decode::from_slice(&block).unwrap();

            let block = query.field_selection.block.prune(block);

            blocks.insert(num, block);
        }

        let data = txs
            .into_values()
            .map(|tx| ResponseRow {
                block: blocks.get(&tx.block_number.unwrap().0).unwrap().clone(),
                transaction: tx,
                log: None,
            })
            .collect();

        let elapsed = start.elapsed().as_millis();

        Ok(QueryResult {
            data,
            metrics: QueryMetrics {
                run_query: elapsed,
                total: elapsed,
                ..Default::default()
            },
        })
    }

    pub fn insert_parquet_idx(&self, dir_name: DirName, idx: &ParquetIdx) -> Result<()> {
        let parquet_idx_cf = self.inner.cf_handle(cf_name::PARQUET_IDX).unwrap();

        let key = key_from_dir_name(dir_name);
        let val = rmp_serde::encode::to_vec(idx).unwrap();

        self.inner
            .put_cf(parquet_idx_cf, key, val)
            .map_err(Error::Db)?;

        self.status
            .parquet_height
            .store(dir_name.range.to, Ordering::Relaxed);

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
        let log_tx_cf = self.inner.cf_handle(cf_name::LOG_TX).unwrap();

        let tail_is_zero = self.status.db_tail.load(Ordering::Relaxed) == 0;

        for (block_range, (blocks, logs)) in block_ranges
            .into_iter()
            .zip(block_batches.into_iter().zip(log_batches.into_iter()))
        {
            let mut batch = rocksdb::WriteBatch::default();

            let mut db_height = self.status.db_height.load(Ordering::Relaxed);
            let mut db_tail = std::u32::MAX;

            for mut block in blocks {
                let txs = mem::take(&mut block.transactions);

                for tx in txs {
                    let val = rmp_serde::encode::to_vec(&tx).unwrap();
                    let tx_key = tx_key(&tx);
                    batch.put_cf(tx_cf, tx_key, &val);
                    batch.put_cf(
                        log_tx_cf,
                        log_tx_key(tx.block_number.0, tx.transaction_index.0),
                        &val,
                    );
                }

                let val = rmp_serde::encode::to_vec(&block).unwrap();
                batch.put_cf(block_cf, block.number.to_be_bytes(), &val);

                db_height = cmp::max(db_height, block.number.0 + 1);
                db_tail = cmp::min(db_tail, block.number.0);
            }

            for log in logs {
                let val = rmp_serde::encode::to_vec(&log).unwrap();
                let log_key = log_key(&log);
                batch.put_cf(log_cf, log_key, &val);
            }

            let start_time = Instant::now();

            self.inner.write(batch).map_err(Error::Db)?;

            let elapsed = start_time.elapsed().as_millis();
            let range = block_range.to - block_range.from;
            if elapsed > 0 && range > 0 {
                self.metrics
                    .record_write_speed(range as f64 / elapsed as f64 * 1000.);
            }

            if db_tail != std::u32::MAX && tail_is_zero {
                self.status.db_tail.store(db_tail, Ordering::Relaxed);
            }
            self.status.db_height.store(db_height, Ordering::Relaxed);

            let height = self.height();
            if height > 0 {
                self.metrics.record_write_height(height - 1);
            }
        }

        Ok(())
    }

    pub fn delete_up_to(&self, to: u32) -> Result<()> {
        let db_height = self.status.db_height.load(Ordering::Relaxed);

        if to >= db_height {
            return Ok(());
        }

        let block_cf = self.inner.cf_handle(cf_name::BLOCK).unwrap();
        let tx_cf = self.inner.cf_handle(cf_name::TX).unwrap();
        let log_cf = self.inner.cf_handle(cf_name::LOG).unwrap();
        let log_tx_cf = self.inner.cf_handle(cf_name::LOG_TX).unwrap();

        let db_tail = self.status.db_tail.load(Ordering::Relaxed);

        for start in (db_tail..to).step_by(500) {
            let end = cmp::min(to, start + 500);

            let mut batch = rocksdb::WriteBatch::default();

            let mut delete_range = |cf| {
                for res in self.inner.iterator_cf(
                    cf,
                    rocksdb::IteratorMode::From(&start.to_be_bytes(), rocksdb::Direction::Forward),
                ) {
                    let (key, _) = res.map_err(Error::Db)?;

                    if key.as_ref() >= end.to_be_bytes().as_slice() {
                        break;
                    }

                    batch.delete_cf(cf, key);
                }

                Ok(())
            };

            delete_range(block_cf)?;
            delete_range(tx_cf)?;
            delete_range(log_cf)?;
            delete_range(log_tx_cf)?;

            self.inner.write(batch).map_err(Error::Db)?;

            self.status.db_tail.store(end, Ordering::Relaxed);
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

        compact(cf_name::BLOCK);
        compact(cf_name::TX);
        compact(cf_name::LOG);
        compact(cf_name::LOG_TX);
        compact(cf_name::PARQUET_IDX);

        log::info!("finished compaction in {}ms", start.elapsed().as_millis());
    }
}

/// Column Family Names
mod cf_name {
    pub const BLOCK: &str = "BLOCK";
    pub const TX: &str = "TX";
    pub const LOG: &str = "LOG";
    pub const LOG_TX: &str = "LOG_TX";
    pub const PARQUET_IDX: &str = "PARQUET_FOLDERS";

    pub const ALL_CF_NAMES: [&str; 5] = [BLOCK, TX, LOG, LOG_TX, PARQUET_IDX];
}

#[derive(Serialize, Deserialize)]
pub struct ParquetIdx {
    pub log_addr_filter: Bloom,
    pub tx_addr_filter: Bloom,
}

fn log_tx_key(block_number: u32, transaction_index: u32) -> [u8; 8] {
    let mut key = [0; 8];

    key[..4].copy_from_slice(&block_number.to_be_bytes());
    key[4..8].copy_from_slice(&transaction_index.to_be_bytes());

    key
}

fn tx_key(tx: &Transaction) -> [u8; 28] {
    tx_key_from_parts(
        tx.block_number.0,
        tx.transaction_index.0,
        match &tx.dest {
            Some(dest) => dest.as_slice(),
            None => &[],
        },
    )
}

fn tx_key_from_parts(block_number: u32, transaction_index: u32, dest: &[u8]) -> [u8; 28] {
    let mut key = [0; 28];

    key[..4].copy_from_slice(&block_number.to_be_bytes());
    key[4..8].copy_from_slice(&transaction_index.to_be_bytes());
    if !dest.is_empty() {
        key[8..].copy_from_slice(dest);
    }

    key
}

fn tx_key_to_dest(key: &[u8]) -> Option<Address> {
    if key.len() == 8 {
        return None;
    }

    Some(Address::new(&key[8..]))
}

fn log_key(log: &Log) -> [u8; 28] {
    let mut key = [0; 28];

    key[..4].copy_from_slice(&log.block_number.to_be_bytes());
    key[4..8].copy_from_slice(&log.log_index.to_be_bytes());
    key[8..].copy_from_slice(log.address.as_slice());

    key
}

fn log_key_to_address(key: &[u8]) -> Address {
    Address::new(&key[8..])
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
    let arr: [u8; 4] = key.try_into().unwrap();

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
