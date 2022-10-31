use cuckoofilter::{CuckooFilter, ExportedCuckooFilter};
use solana_bloom::bloom::Bloom;
use std::path::Path;
use std::convert::TryInto;
use std::sync::atomic::{Ordering, AtomicU32};

pub struct DbHandle {
    inner: rocksdb::OptimisticTransactionDB,
    status: Status,
}

pub struct Status {
    parquet_height: AtomicU32,
    db_height: AtomicU32,
    db_tail: AtomicU32,
}

impl DbHandle {
    pub async fn new<P: AsRef<Path>>(path: P) -> Result<DbHandle> {
        let mut opts = rocksdb::Options::default();

        let (inner, status) = tokio::spawn_blocking(move || {
            let inner =
                rocksdb::OptimisticTransactionDB::open_cf(&opts, path, cf_name::ALL_CF_NAMES)
                    .map_err(Error::OpenDb)?;

            let status = Self::get_status(&inner).map_err(Error::GetMaxBlock)?;

            Ok((inner, status))
        });

        Ok(Self { inner, status })
    }

    pub fn get_next_parquet_folder(
        &self,
        from_block: u32,
    ) -> Result<Option<DirName>> {
        todo!()
    }

    pub fn query(&self, query: MiniQuery) -> Result<QueryResult> {
        todo!()
    }

    pub fn insert_parquet_idx(&self, dir_name: DirName, idx: &ParquetIdx) -> Result<()> {
        let key = key_from_dir_name(dir_name);
        let val = rmp_serde::encode::to_vec(idx).map_err(Error::MsgPack)?;

        self.inner.put_cf(parquet_idx_cf, &key, &val).map_err(Error::Db)?;

        self.status.parquet_height.store(Ordering::Relaxed, dir_name.range.to);

        Ok(())
    }

    pub fn insert_batches(&self, (block_batches, log_batches): (Vec<Vec<Block>>, Vec<Vec<Log>>)) -> Result<()> {
        let block_cf = self.inner.cf_handle(cf_name::BLOCK).unwrap();
        let tx_cf = self.inner.cf_handle(cf_name::TX).unwrap();
        let log_cf = self.inner.cf_handle(cf_name::LOG).unwrap();
        let addr_tx_cf = self.inner.cf_handle(cf_name::ADDR_TX).unwrap();
        let log_tx_cf = self.inner.cf_handle(cf_name::LOG_TX).unwrap();

        for (blocks, logs) in block_batches.into_iter().zip(log_batches.into_iter()) {
            let db_tx = self.inner.transaction();

            let mut db_height = self.status.db_height.load(Ordering::Relaxed);

            for mut block in blocks {
                let txs = mem::take(&mut block.transactions);

                for tx in txs {
                    let val = rmp_serde::encode::to_vec(&tx).map_err(Error::MsgPack)?;
                    let tx_key = tx_key(&tx);
                    db_tx.put_cf(tx_cf, &tx_key, &val).map_err(Error::Db)?;
                    db_tx.put_cf(addr_tx_cf, &addr_tx_key(&tx), &tx_key).map_err(Error::Db)?;
                }

                let val = rmp_serde::encode::to_vec(&block).map_err(Error::MsgPack)?;
                db_tx.put_cf(block_cf, &block.number.as_be_bytes(), &val).map_err(Error::Db)?;

                db_height = block.number;
            }

            for log in logs {
                let val = rmp_serde::encode::to_vec(&log).map_err(Error::MsgPack)?;
                let log_key = log_key(&log);
                db_tx.put_cf(log_cf, &log_key, &val).map_err(Error::Db)?;
                db_tx.put_cf(addr_log_cf, &addr_log_key(&log), &log_key).map_err(Error::Db)?;
            }

            db_tx.commit().map_err(Error::Db)?;

            self.status.db_height.store(db_height, Ordering::Relaxed);
        }
    }

    pub fn delete_tail(&self) -> Result<()> {
        todo!()
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

    fn get_status(inner: &rocksdb::OptimisticTransactionDB) -> Result<Status> {
        let parquet_idx_cf = inner.cf_handle(cf_name::PARQUET_IDX).unwrap();

        let parquet_height = inner.iterator_cf(parquet_idx_cf, rocksdb::IteratorMode::End)
            .next()
            .transpose()
            .map_err(Error::Db)?
            .map(|(key, _)| {
                dir_name_from_key(&key).range.to
            })
            .unwrap_or(0);

        let block_cf = inner.cf_handle(cf_name::BLOCK).unwrap();

        let db_tail = inner.iterator_cf(block_cf, rocksdb::IteratorMode::Start)
            .next()
            .transpose()
            .map_err(Error::Db)?
            .map(|(key, _)| u32::from_be_bytes(key.try_into().unwrap()))
            .unwrap_or(0);

        let db_height = inner.iterator_cf(block_cf, rocksdb::IteratorMode::End)
            .next()
            .transpose()
            .map_err(Error::Db)?
            .map(|(key, _)| u32::from_be_bytes(key.try_into().unwrap()))
            .unwrap_or(0);

        Ok(Status {
            parquet_height: AtomicU32::new(parquet_height),
            db_tail: AtomicU32::new(db_tail),
            db_height: AtomicU32::new(db_height),
        })
    }
}

pub const LOG_ADDR_FILTER: &str = "LOG_ADDR_FILTER";
pub const TX_ADDR_FILTER: &str = "TX_ADDR_FILTER";

/// Column Family Names
mod cf_name {
    pub const BLOCK: &str = "BLOCK";
    pub const TX: &str = "TX";
    pub const LOG: &str = "LOG";
    pub const ADDR_LOG: &str = "ADDR_LOG";
    pub const ADDR_TX: &str = "ADDR_TX";
    pub const PARQUET_IDX: &str = "PARQUET_FOLDERS";

    pub const ALL_CF_NAMES: [&str; 6] = [
        BLOCK,
        TX,
        LOG,
        ADDR_LOG,
        ADDR_TX,
        PARQUET_IDX,
    ];
}

#[derive(Serialize, Deserialize)]
pub struct ParquetIdx {
    pub log_addr_filter: Bloom,
    pub tx_addr_filter: Bloom,
}

#[derive(Serialize, Deserialize)]
struct ExportedCuckoo {
    values: Vec<u8>,
    length: usize,
}

impl From<&CuckooFilter> for ExportedCuckoo {
    fn from(filter: &CuckooFilter) -> Self {
        let filter = filter.export();

        Self {
            values: filter.values,
            length: filter.length,
        }
    }
}

impl From<ExportedCuckoo> for CuckooFilter {
    fn from(filter: ExportedCuckoo) -> Self {
        Self::from(ExportedCuckooFilter {
            values: filter.values,
            length: filter.length,
        })
    }
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

    (&mut key[0..20]).copy_from_slice(tx.dest.as_slice());
    (&mut key[20..24]).copy_from_slice(&tx.block_number.to_be_bytes());
    (&mut key[24..]).copy_from_slice(&tx.transaction_index.to_be_bytes());

    key
}

fn addr_log_key(tx: &Log) -> [u8; 28] {
    let mut key = [0; 28];

    (&mut key[0..20]).copy_from_slice(log.address.as_slice());
    (&mut key[20..24]).copy_from_slice(&log.block_number.to_be_bytes());
    (&mut key[24..]).copy_from_slice(&log.log_index.to_be_bytes());

    key
}

fn dir_name_from_key(key: &[u8]) -> DirName {
    let num = u64::from_be_bytes(key.try_into().unwrap());
    let to = num as u32;
    let from = (num >> 32) as u32;
    DirName {
        range: BlockRange { from, to },
        is_temp: false,
    }
}

fn key_from_dir_name(dir_name: DirName) -> [u8; 8] {
    assert_eq!(dir_name.is_temp, false);

    let num = (u64::from(dir_name.range.from) << 32) | u64::from(dir_name.range.to);
    num.to_be_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dir_name_key_smoke() {
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
