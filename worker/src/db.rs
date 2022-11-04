use crate::types::MiniQuery;
use crate::{Error, Result};
use eth_archive_core::deserialize::Address;
use eth_archive_core::dir_name::DirName;
use eth_archive_core::types::{
    Block, BlockRange, Log, QueryMetrics, QueryResult, ResponseBlock, ResponseLog, ResponseRow,
    ResponseTransaction, Transaction,
};
use serde::{Deserialize, Serialize};
use solana_bloom::bloom::Bloom as BloomFilter;
use std::convert::TryInto;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::{cmp, iter, mem};

type Bloom = BloomFilter<Address>;

pub struct DbHandle {
    inner: rocksdb::OptimisticTransactionDB,
    status: Status,
}

struct Status {
    parquet_height: AtomicU32,
    db_height: AtomicU32,
    db_tail: AtomicU32,
}

impl DbHandle {
    pub async fn new(path: &PathBuf) -> Result<DbHandle> {
        let path = path.clone();

        let (inner, status) = tokio::task::spawn_blocking(move || {
            let mut opts = rocksdb::Options::default();

            opts.create_if_missing(true);
            opts.create_missing_column_families(true);
            opts.set_compression_type(rocksdb::DBCompressionType::Lz4);

            let inner =
                rocksdb::OptimisticTransactionDB::open_cf(&opts, path, cf_name::ALL_CF_NAMES)
                    .map_err(Error::OpenDb)?;

            let status = Self::get_status(&inner)?;

            Ok((inner, status))
        })
        .await
        .map_err(Error::TaskJoinError)??;

        Ok(Self { inner, status })
    }

    pub fn iter_parquet_idxs(
        &self,
        from: u32,
        to: Option<u32>,
    ) -> Result<Box<dyn Iterator<Item = Result<(DirName, ParquetIdx)>>>> {
        let parquet_idx_cf = self.inner.cf_handle(cf_name::PARQUET_IDX).unwrap();

        let mut iter = self.inner.iterator_cf(
            parquet_idx_cf,
            rocksdb::IteratorMode::From(&from.to_be_bytes(), rocksdb::Direction::Reverse),
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
                let idx = rmp_serde::decode::from_read_ref(&idx).unwrap();

                Ok((dir_name, idx))
            })
            .take_while(|res| {
                let (dir_name, _) = match res {
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
        todo!()
    }

    pub fn insert_parquet_idx(&self, dir_name: DirName, idx: &ParquetIdx) -> Result<()> {
        let parquet_idx_cf = self.inner.cf_handle(cf_name::PARQUET_IDX).unwrap();

        let key = key_from_dir_name(dir_name);
        let val = rmp_serde::encode::to_vec(idx).unwrap();

        self.inner
            .put_cf(parquet_idx_cf, &key, &val)
            .map_err(Error::Db)?;

        self.status
            .parquet_height
            .store(dir_name.range.to, Ordering::Relaxed);

        Ok(())
    }

    pub fn insert_batches(
        &self,
        (block_batches, log_batches): (Vec<Vec<Block>>, Vec<Vec<Log>>),
    ) -> Result<()> {
        let block_cf = self.inner.cf_handle(cf_name::BLOCK).unwrap();
        let tx_cf = self.inner.cf_handle(cf_name::TX).unwrap();
        let log_cf = self.inner.cf_handle(cf_name::LOG).unwrap();
        let addr_tx_cf = self.inner.cf_handle(cf_name::ADDR_TX).unwrap();
        let addr_log_cf = self.inner.cf_handle(cf_name::ADDR_LOG).unwrap();

        let tail_is_zero = self.status.db_tail.load(Ordering::Relaxed) == 0;

        for (blocks, logs) in block_batches.into_iter().zip(log_batches.into_iter()) {
            let db_tx = self.inner.transaction();

            let mut db_height = self.status.db_height.load(Ordering::Relaxed);
            let mut db_tail = std::u32::MAX;

            for mut block in blocks {
                let txs = mem::take(&mut block.transactions);

                for tx in txs {
                    let val = rmp_serde::encode::to_vec(&tx).unwrap();
                    let tx_key = tx_key(&tx);
                    db_tx.put_cf(tx_cf, &tx_key, &val).map_err(Error::Db)?;
                    db_tx
                        .put_cf(addr_tx_cf, &addr_tx_key(&tx), &tx_key)
                        .map_err(Error::Db)?;
                }

                let val = rmp_serde::encode::to_vec(&block).unwrap();
                db_tx
                    .put_cf(block_cf, &block.number.to_be_bytes(), &val)
                    .map_err(Error::Db)?;

                db_height = cmp::max(db_height, block.number.0 + 1);
                db_tail = cmp::min(db_tail, block.number.0);
            }

            for log in logs {
                let val = rmp_serde::encode::to_vec(&log).unwrap();
                let log_key = log_key(&log);
                db_tx.put_cf(log_cf, &log_key, &val).map_err(Error::Db)?;
                db_tx
                    .put_cf(addr_log_cf, &addr_log_key(&log), &log_key)
                    .map_err(Error::Db)?;
            }

            db_tx.commit().map_err(Error::Db)?;

            if db_tail != std::u32::MAX && tail_is_zero {
                self.status.db_tail.store(db_tail, Ordering::Relaxed);
            }

            self.status.db_height.store(db_height, Ordering::Relaxed);
        }

        Ok(())
    }

    pub fn delete_up_to(&self, to: u32) -> Result<()> {
        let db_height = self.status.db_height.load(Ordering::Relaxed);

        if to >= db_height {
            panic!("invalid 'to' argument passed to delete_up_to: {}", to);
        }

        let block_cf = self.inner.cf_handle(cf_name::BLOCK).unwrap();
        let tx_cf = self.inner.cf_handle(cf_name::TX).unwrap();
        let log_cf = self.inner.cf_handle(cf_name::LOG).unwrap();
        let addr_tx_cf = self.inner.cf_handle(cf_name::ADDR_TX).unwrap();
        let addr_log_cf = self.inner.cf_handle(cf_name::ADDR_LOG).unwrap();

        let db_tail = self.status.db_tail.load(Ordering::Relaxed);
        for block_num in db_tail..to {
            let mut addr_tx_keys = Vec::new();
            let mut addr_log_keys = Vec::new();

            for res in self
                .inner
                .prefix_iterator_cf(tx_cf, &block_num.to_be_bytes())
            {
                let (_, tx) = res.map_err(Error::Db)?;
                let tx = rmp_serde::decode::from_read_ref(&tx).unwrap();

                addr_tx_keys.push(addr_tx_key(&tx));
            }

            for res in self
                .inner
                .prefix_iterator_cf(log_cf, &block_num.to_be_bytes())
            {
                let (_, log) = res.map_err(Error::Db)?;
                let log = rmp_serde::decode::from_read_ref(&log).unwrap();

                addr_log_keys.push(addr_log_key(&log));
            }

            let mut batch = rocksdb::WriteBatchWithTransaction::<false>::default();

            for key in addr_tx_keys {
                batch.delete_cf(addr_tx_cf, &key);
            }
            for key in addr_log_keys {
                batch.delete_cf(addr_log_cf, &key);
            }

            let from = block_num.to_be_bytes();
            let to = (block_num + 1).to_be_bytes();

            batch.delete_cf(block_cf, &from);

            batch.delete_range_cf(tx_cf, &from, &to);
            batch.delete_range_cf(log_cf, &from, &to);

            self.inner.write(batch).map_err(Error::Db)?;

            self.status.db_tail.store(block_num + 1, Ordering::Relaxed);
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

    fn get_status(inner: &rocksdb::OptimisticTransactionDB) -> Result<Status> {
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
            .map(|(key, _)| u32::from_be_bytes((*key).try_into().unwrap()))
            .unwrap_or(0);

        let db_height = inner
            .iterator_cf(block_cf, rocksdb::IteratorMode::End)
            .next()
            .transpose()
            .map_err(Error::Db)?
            .map(|(key, _)| u32::from_be_bytes((*key).try_into().unwrap() + 1))
            .unwrap_or(0);

        Ok(Status {
            parquet_height: AtomicU32::new(parquet_height),
            db_tail: AtomicU32::new(db_tail),
            db_height: AtomicU32::new(db_height),
        })
    }
}

/// Column Family Names
mod cf_name {
    pub const BLOCK: &str = "BLOCK";
    pub const TX: &str = "TX";
    pub const LOG: &str = "LOG";
    pub const ADDR_LOG: &str = "ADDR_LOG";
    pub const ADDR_TX: &str = "ADDR_TX";
    pub const PARQUET_IDX: &str = "PARQUET_FOLDERS";

    pub const ALL_CF_NAMES: [&str; 6] = [BLOCK, TX, LOG, ADDR_LOG, ADDR_TX, PARQUET_IDX];
}

#[derive(Serialize, Deserialize)]
pub struct ParquetIdx {
    pub log_addr_filter: Bloom,
    pub tx_addr_filter: Bloom,
}

fn tx_key(tx: &Transaction) -> [u8; 8] {
    let mut key = [0; 8];

    (&mut key[..4]).copy_from_slice(&tx.block_number.to_be_bytes());
    (&mut key[4..]).copy_from_slice(&tx.transaction_index.to_be_bytes());

    key
}

fn log_key(log: &Log) -> [u8; 8] {
    let mut key = [0; 8];

    (&mut key[..4]).copy_from_slice(&log.block_number.to_be_bytes());
    (&mut key[4..]).copy_from_slice(&log.log_index.to_be_bytes());

    key
}

fn addr_tx_key(tx: &Transaction) -> [u8; 28] {
    let mut key = [0; 28];

    (&mut key[..20]).copy_from_slice(tx.dest.as_slice());
    (&mut key[20..24]).copy_from_slice(&tx.block_number.to_be_bytes());
    (&mut key[24..]).copy_from_slice(&tx.transaction_index.to_be_bytes());

    key
}

fn addr_log_key(log: &Log) -> [u8; 28] {
    let mut key = [0; 28];

    (&mut key[..20]).copy_from_slice(log.address.as_slice());
    (&mut key[20..24]).copy_from_slice(&log.block_number.to_be_bytes());
    (&mut key[24..]).copy_from_slice(&log.log_index.to_be_bytes());

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
    assert_eq!(dir_name.is_temp, false);

    let mut key = [0; 8];

    (&mut key[..4]).copy_from_slice(dir_name.range.from.to_be_bytes());
    (&mut key[4..]).copy_from_slice(dir_name.range.to.to_be_bytes());

    key
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
