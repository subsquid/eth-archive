use crate::types::{MiniLogSelection, MiniQuery, MiniTransactionSelection};
use crate::{Error, Result};
use eth_archive_core::deserialize::{Address, Bytes, Bytes32, Index};
use eth_archive_core::dir_name::DirName;
use eth_archive_core::ingest_metrics::IngestMetrics;
use eth_archive_core::types::{
    Block, BlockRange, Log, QueryResult, ResponseBlock, ResponseRow, Transaction,
};
use futures::Stream;
use libmdbx::{Database, NoWriteMap};
use std::collections::{BTreeMap, BTreeSet};
use std::convert::TryInto;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::mpsc;

pub struct DbHandle {
    inner: Database<NoWriteMap>,
    metrics: Arc<IngestMetrics>,
}

impl DbHandle {
    pub async fn new(path: &Path, metrics: Arc<IngestMetrics>) -> Result<DbHandle> {
        let path = path.to_owned();
        tokio::task::spawn_blocking(move || Self::new_impl(path, metrics))
            .await
            .unwrap()
    }

    fn new_impl(path: PathBuf, metrics: Arc<IngestMetrics>) -> Result<DbHandle> {
        let inner = Database::new()
            .set_max_tables(table_name::ALL_TABLE_NAMES.len())
            .open(&path)
            .map_err(Error::OpenDb)?;

        let txn = inner.begin_rw_txn().map_err(Error::Db)?;
        for table in table_name::ALL_TABLE_NAMES.iter() {
            txn.create_table(Some(*table), Default::default())
                .map_err(Error::Db)?;
        }
        txn.commit().map_err(Error::Db)?;

        Ok(Self { inner, metrics })
    }

    pub async fn iter_parquet_idxs(
        self: Arc<Self>,
        from: u32,
        to: Option<u32>,
    ) -> Result<mpsc::Receiver<Result<(DirName, ParquetIdx)>>> {
        let iter = tokio::task::spawn_blocking(move || self.iter_parquet_idxs_impl(from, to))
            .await
            .unwrap()?;

        let (tx, rx): (mpsc::Sender<Result<(DirName, ParquetIdx)>>, _) = mpsc::channel(1);

        tokio::task::spawn_blocking(move || {
            for res in iter {
                if tx.blocking_send(res).is_err() {
                    break;
                }
            }
        });

        Ok(rx)
    }

    fn iter_parquet_idxs_impl(
        &self,
        from: u32,
        to: Option<u32>,
    ) -> Result<impl Iterator<Item = Result<(DirName, ParquetIdx)>>> {
        let key = key_from_dir_name(DirName {
            range: BlockRange { from, to: 0 },
            is_temp: false,
        });

        let txn = self.inner.begin_ro_txn().map_err(Error::Db)?;
        let parquet_idx_table = txn
            .open_table(Some(table_name::PARQUET_IDX))
            .map_err(Error::Db)?;
        let cursor = txn.cursor(&parquet_idx_table).map_err(Error::Db)?;

        let iter = cursor
            .into_iter_from(&key)
            .map(|res| {
                let (dir_name, idx): (Vec<u8>, Vec<u8>) = res.map_err(Error::Db)?;

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
        let mut data = vec![];

        if !query.logs.is_empty() {
            let logs = self.query_logs(&query)?;
            data.extend_from_slice(&logs.data);
        }

        if !query.transactions.is_empty() {
            let transactions = self.query_transactions(&query)?;
            data.extend_from_slice(&transactions.data);
        }

        Ok(QueryResult { data })
    }

    fn query_logs(&self, query: &MiniQuery) -> Result<QueryResult> {
        let txn = self.inner.begin_ro_txn().map_err(Error::Db)?;

        let log_table = txn.open_table(Some(table_name::LOG)).map_err(Error::Db)?;
        let log_tx_table = txn
            .open_table(Some(table_name::LOG_TX))
            .map_err(Error::Db)?;

        let mut block_nums = BTreeSet::new();
        let mut tx_keys = BTreeSet::new();
        let mut logs = BTreeMap::new();

        for res in txn
            .cursor(&log_table)
            .map_err(Error::Db)?
            .into_iter_from(&query.from_block.to_be_bytes())
        {
            let (log_key, log): (Vec<u8>, Vec<u8>) = res.map_err(Error::Db)?;

            if log_key.as_slice() >= query.to_block.to_be_bytes().as_slice() {
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

        let blocks = self.get_blocks(&block_nums, query)?;

        let mut txs = BTreeMap::new();
        for key in tx_keys {
            let tx: Vec<u8> = txn.get(&log_tx_table, &key).map_err(Error::Db)?.unwrap();
            let tx = rmp_serde::decode::from_slice(&tx).unwrap();

            let tx = query.field_selection.transaction.prune(tx);

            txs.insert(key, tx);
        }

        let mut data = logs
            .into_values()
            .map(|log| ResponseRow {
                block: blocks.get(&log.block_number.unwrap().0).unwrap().clone(),
                transaction: txs
                    .get(&log_tx_key(
                        log.block_number.unwrap().0,
                        log.transaction_index.unwrap().0,
                    ))
                    .cloned(),
                log: Some(log),
            })
            .collect::<Vec<_>>();

        if query.include_all_blocks {
            for block in blocks.into_values() {
                data.push(ResponseRow {
                    block,
                    transaction: None,
                    log: None,
                })
            }
        }

        Ok(QueryResult { data })
    }

    fn query_transactions(&self, query: &MiniQuery) -> Result<QueryResult> {
        let txn = self.inner.begin_ro_txn().map_err(Error::Db)?;

        let tx_table = txn.open_table(Some(table_name::TX)).map_err(Error::Db)?;

        let mut block_nums = BTreeSet::new();
        let mut txs = BTreeMap::new();

        for res in txn
            .cursor(&tx_table)
            .map_err(Error::Db)?
            .into_iter_from(&query.from_block.to_be_bytes())
        {
            let (tx_key, tx): (Vec<u8>, Vec<u8>) = res.map_err(Error::Db)?;

            if tx_key.as_slice() >= query.to_block.to_be_bytes().as_slice() {
                break;
            }

            let tx: Transaction = rmp_serde::decode::from_slice(&tx).unwrap();

            if !query.matches_tx(&tx) {
                continue;
            }

            block_nums.insert(tx.block_number.0);

            let tx = query.field_selection.transaction.prune(tx);
            txs.insert(tx_key, tx);
        }

        let blocks = self.get_blocks(&block_nums, query)?;

        let mut data = txs
            .into_values()
            .map(|tx| ResponseRow {
                block: blocks.get(&tx.block_number.unwrap().0).unwrap().clone(),
                transaction: Some(tx),
                log: None,
            })
            .collect::<Vec<_>>();

        if query.include_all_blocks {
            for block in blocks.into_values() {
                data.push(ResponseRow {
                    block,
                    transaction: None,
                    log: None,
                })
            }
        }

        Ok(QueryResult { data })
    }

    fn get_blocks(
        &self,
        block_nums: &BTreeSet<u32>,
        query: &MiniQuery,
    ) -> Result<BTreeMap<u32, ResponseBlock>> {
        let txn = self.inner.begin_ro_txn().map_err(Error::Db)?;

        let block_table = txn.open_table(Some(table_name::BLOCK)).map_err(Error::Db)?;

        let mut blocks = BTreeMap::new();

        if !query.include_all_blocks {
            for num in block_nums {
                let block: Vec<u8> = txn
                    .get(&block_table, &num.to_be_bytes())
                    .map_err(Error::Db)?
                    .unwrap();
                let block = rmp_serde::decode::from_slice(&block).unwrap();

                let block = query.field_selection.block.prune(block);

                blocks.insert(*num, block);
            }
        } else {
            for res in txn
                .cursor(&block_table)
                .map_err(Error::Db)?
                .into_iter_from(&query.from_block.to_be_bytes())
            {
                let (block_key, block): (Vec<u8>, Vec<u8>) = res.map_err(Error::Db)?;

                let block_num = u32::from_be_bytes((&*block_key).try_into().unwrap());

                if block_num >= query.to_block {
                    break;
                }

                let block: Block = rmp_serde::decode::from_slice(&block).unwrap();
                let block = query.field_selection.block.prune(block);

                blocks.insert(block_num, block);
            }
        }

        Ok(blocks)
    }

    pub fn insert_parquet_idx(&self, dir_name: DirName, idx: &ParquetIdx) -> Result<()> {
        let txn = self.inner.begin_rw_txn().map_err(Error::Db)?;

        let parquet_idx_table = txn
            .open_table(Some(table_name::PARQUET_IDX))
            .map_err(Error::Db)?;

        let key = key_from_dir_name(dir_name);

        let log_val = rmp_serde::encode::to_vec(idx).unwrap();

        txn.put(&parquet_idx_table, key, &log_val, Default::default())
            .map_err(Error::Db)?;

        for table in [
            table_name::BLOCK,
            table_name::TX,
            table_name::LOG,
            table_name::LOG_TX,
        ] {
            let table = txn.open_table(Some(table)).map_err(Error::Db)?;

            for res in txn.cursor(&table).map_err(Error::Db)?.into_iter_start() {
                let (key, _): (Vec<u8>, Vec<u8>) = res.map_err(Error::Db)?;
                if key.as_slice() >= dir_name.range.to.to_be_bytes().as_slice() {
                    break;
                }

                txn.del(&table, key, None).map_err(Error::Db)?;
            }
        }

        txn.commit().map_err(Error::Db)?;

        assert!(dir_name.range.to > 0);
        self.metrics.record_write_height(dir_name.range.to - 1);

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
        let txn = self.inner.begin_rw_txn().map_err(Error::Db)?;

        let log_table = txn.open_table(Some(table_name::LOG)).map_err(Error::Db)?;
        let tx_table = txn.open_table(Some(table_name::TX)).map_err(Error::Db)?;
        let log_tx_table = txn
            .open_table(Some(table_name::LOG_TX))
            .map_err(Error::Db)?;
        let block_table = txn.open_table(Some(table_name::BLOCK)).map_err(Error::Db)?;

        for (_, (blocks, logs)) in block_ranges
            .into_iter()
            .zip(block_batches.into_iter().zip(log_batches.into_iter()))
        {
            for block in blocks.iter() {
                let val = rmp_serde::encode::to_vec(block).unwrap();
                txn.put(
                    &block_table,
                    block.number.to_be_bytes(),
                    &val,
                    Default::default(),
                )
                .map_err(Error::Db)?;

                for tx in block.transactions.iter() {
                    let val = rmp_serde::encode::to_vec(tx).unwrap();
                    let tx_key = tx_key(tx);

                    let log_tx_key = log_tx_key(tx.block_number.0, tx.transaction_index.0);

                    txn.put(&tx_table, tx_key, &val, Default::default())
                        .map_err(Error::Db)?;
                    txn.put(&log_tx_table, log_tx_key, &val, Default::default())
                        .map_err(Error::Db)?;
                }
            }

            for log in logs {
                let val = rmp_serde::encode::to_vec(&log).unwrap();
                let log_key = log_key(&log);
                txn.put(&log_table, log_key, &val, Default::default())
                    .map_err(Error::Db)?;
            }
        }

        txn.commit().map_err(Error::Db)?;

        Ok(())
    }

    pub async fn height(self: Arc<Self>) -> Result<u32> {
        tokio::task::spawn_blocking(move || self.height_impl())
            .await
            .unwrap()
    }

    fn height_impl(&self) -> Result<u32> {
        let txn = self.inner.begin_ro_txn().map_err(Error::Db)?;

        let block_table = txn.open_table(Some(table_name::BLOCK)).map_err(Error::Db)?;
        let parquet_idx_table = txn
            .open_table(Some(table_name::PARQUET_IDX))
            .map_err(Error::Db)?;

        let opt: Option<(Vec<u8>, Vec<u8>)> = txn
            .cursor(&parquet_idx_table)
            .map_err(Error::Db)?
            .last()
            .map_err(Error::Db)?;

        let parquet_height = opt
            .map(|(dir_name, _)| dir_name_from_key(&dir_name).range.to)
            .unwrap_or(0);

        let opt: Option<(Vec<u8>, Vec<u8>)> = txn
            .cursor(&block_table)
            .map_err(Error::Db)?
            .first()
            .map_err(Error::Db)?;

        let hot_data_tail = opt
            .map(|(hot_data_tail, _)| block_num_from_key(&hot_data_tail))
            .unwrap_or(0);

        if parquet_height >= hot_data_tail {
            let opt: Option<(Vec<u8>, Vec<u8>)> = txn
                .cursor(&block_table)
                .map_err(Error::Db)?
                .first()
                .map_err(Error::Db)?;

            let hot_data_head = opt
                .map(|(hot_data_head, _)| block_num_from_key(&hot_data_head))
                .unwrap_or(0);

            Ok(hot_data_head)
        } else {
            Ok(parquet_height)
        }
    }

    pub async fn hot_data_height(self: Arc<Self>) -> Result<u32> {
        tokio::task::spawn_blocking(move || self.hot_data_height_impl())
            .await
            .unwrap()
    }

    fn hot_data_height_impl(&self) -> Result<u32> {
        let txn = self.inner.begin_ro_txn().map_err(Error::Db)?;
        let block_table = txn.open_table(Some(table_name::BLOCK)).map_err(Error::Db)?;

        let opt: Option<(Vec<u8>, Vec<u8>)> = txn
            .cursor(&block_table)
            .map_err(Error::Db)?
            .first()
            .map_err(Error::Db)?;

        let hot_data_head = opt
            .map(|(hot_data_head, _)| block_num_from_key(&hot_data_head))
            .unwrap_or(0);

        Ok(hot_data_head)
    }

    pub async fn parquet_height(self: Arc<Self>) -> Result<u32> {
        tokio::task::spawn_blocking(move || self.parquet_height_impl())
            .await
            .unwrap()
    }

    fn parquet_height_impl(&self) -> Result<u32> {
        let txn = self.inner.begin_ro_txn().map_err(Error::Db)?;
        let parquet_idx_table = txn
            .open_table(Some(table_name::PARQUET_IDX))
            .map_err(Error::Db)?;

        let opt: Option<(Vec<u8>, Vec<u8>)> = txn
            .cursor(&parquet_idx_table)
            .map_err(Error::Db)?
            .last()
            .map_err(Error::Db)?;

        let parquet_height = opt
            .map(|(dir_name, _)| dir_name_from_key(&dir_name).range.to)
            .unwrap_or(0);

        Ok(parquet_height)
    }
}

/// Column Family Names
mod table_name {
    pub const BLOCK: &str = "BLOCK";
    pub const TX: &str = "TX";
    pub const LOG: &str = "LOG";
    pub const LOG_TX: &str = "LOG_TX";
    pub const PARQUET_IDX: &str = "PARQUET_IDX";

    pub const ALL_TABLE_NAMES: [&str; 5] = [BLOCK, TX, LOG, LOG_TX, PARQUET_IDX];
}

pub type ParquetIdx = xorf::BinaryFuse8;

fn log_tx_key(block_number: u32, transaction_index: u32) -> [u8; 8] {
    let mut key = [0; 8];

    key[..4].copy_from_slice(&block_number.to_be_bytes());
    key[4..8].copy_from_slice(&transaction_index.to_be_bytes());

    key
}

fn tx_key(tx: &Transaction) -> [u8; 8] {
    let mut key = [0; 8];

    key[..4].copy_from_slice(&tx.block_number.0.to_be_bytes());
    key[4..].copy_from_slice(&tx.transaction_index.0.to_be_bytes());

    key
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

impl MiniQuery {
    fn matches_log_addr(&self, addr: &Address) -> bool {
        self.logs
            .iter()
            .any(|selection| selection.matches_addr(addr))
    }

    fn matches_log(&self, log: &Log) -> bool {
        self.logs.iter().any(|selection| {
            selection.matches_addr(&log.address) && selection.matches_topics(&log.topics)
        })
    }

    #[allow(clippy::match_like_matches_macro)]
    fn matches_tx(&self, tx: &Transaction) -> bool {
        self.transactions.iter().any(|selection| {
            let match_all_addr = selection.source.is_empty() && selection.dest.is_empty();
            let matches_addr = match_all_addr
                || selection.matches_dest(&tx.dest)
                || selection.matches_source(&tx.source);

            matches_addr
                && selection.matches_sighash(&tx.input)
                && selection.matches_status(tx.status)
        })
    }
}

impl MiniLogSelection {
    fn matches_addr(&self, filter_addr: &Address) -> bool {
        if !self.address.is_empty() && !self.address.iter().any(|addr| addr == filter_addr) {
            return false;
        }

        true
    }

    fn matches_topics(&self, topics: &[Bytes32]) -> bool {
        for (topic, log_topic) in self.topics.iter().zip(topics.iter()) {
            if !topic.is_empty() && !topic.iter().any(|topic| log_topic == topic) {
                return false;
            }
        }

        true
    }
}

impl MiniTransactionSelection {
    fn matches_dest(&self, dest: &Option<Address>) -> bool {
        let tx_addr = match dest.as_ref() {
            Some(addr) => addr,
            None => return false,
        };

        self.dest.iter().any(|addr| addr == tx_addr)
    }

    fn matches_source(&self, source: &Option<Address>) -> bool {
        let tx_addr = match source.as_ref() {
            Some(addr) => addr,
            None => return false,
        };

        self.source.iter().any(|addr| addr == tx_addr)
    }

    fn matches_sighash(&self, input: &Bytes) -> bool {
        let input = match input.get(..4) {
            Some(sig) => sig,
            None => return false,
        };

        self.sighash.is_empty() || self.sighash.iter().any(|sig| sig.as_slice() == input)
    }

    fn matches_status(&self, tx_status: Option<Index>) -> bool {
        match (self.status, tx_status) {
            (Some(status), Some(tx_status)) => status == tx_status.0,
            _ => true,
        }
    }
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
