use crate::db::{Bloom, DbHandle, ParquetIdx};
use crate::{Error, Result};
use eth_archive_core::dir_name::DirName;
use eth_archive_core::types::{Block, Log};
use polars::export::arrow::array::BinaryArray;
use polars::prelude::*;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use eth_archive_core::deserialize::Address;

pub struct DbWriter {
    tx: mpsc::Sender<Job>,
}

impl DbWriter {
    pub fn new(db: Arc<DbHandle>, data_path: &Path, min_hot_block_range: u32) -> Self {
        let (tx, mut rx) = mpsc::channel::<Job>(2);

        let data_path = data_path.to_owned();

        std::thread::spawn(move || {
            while let Some(job) = rx.blocking_recv() {
                let res = match job.kind {
                    JobKind::WriteBatches(batches) => {
                        Self::handle_write_batches(&db, batches, min_hot_block_range)
                    }
                    JobKind::RegisterParquetFolder(dir_name) => {
                        Self::handle_register_parquet_folder(&db, &data_path, dir_name)
                    }
                };

                job.respond.send(res).ok().unwrap();
            }
        });

        Self { tx }
    }

    pub async fn write_batches(&self, batches: (Vec<Vec<Block>>, Vec<Vec<Log>>)) -> Result<()> {
        let (tx, rx) = oneshot::channel();

        self.tx
            .send(Job {
                respond: tx,
                kind: JobKind::WriteBatches(batches),
            })
            .await
            .ok()
            .unwrap();

        rx.await.unwrap()
    }

    pub async fn register_parquet_folder(&self, dir_name: DirName) -> Result<()> {
        let (tx, rx) = oneshot::channel();

        self.tx
            .send(Job {
                respond: tx,
                kind: JobKind::RegisterParquetFolder(dir_name),
            })
            .await
            .ok()
            .unwrap();

        rx.await.unwrap()
    }

    fn handle_write_batches(
        db: &DbHandle,
        batches: (Vec<Vec<Block>>, Vec<Vec<Log>>),
        min_hot_block_range: u32,
    ) -> Result<()> {
        db.insert_batches(batches)?;
        let height = db.height();
        if height > min_hot_block_range {
            db.delete_up_to(height - min_hot_block_range)?;
        }

        Ok(())
    }

    #[allow(clippy::manual_flatten)]
    fn handle_register_parquet_folder(
        db: &DbHandle,
        data_path: &Path,
        dir_name: DirName,
    ) -> Result<()> {
        let log_addr_filter = {
            let mut path = data_path.to_owned();
            path.push(dir_name.to_string());
            path.push("log.parquet");

            let lazy_frame =
                LazyFrame::scan_parquet(&path, Default::default()).map_err(Error::ScanParquet)?;
            let data_frame = lazy_frame
                .select(vec![col("address")])
                .unique(None, UniqueKeepStrategy::First)
                .collect()
                .map_err(Error::ExecuteQuery)?;

            let mut addrs = Vec::new();

            for chunk in data_frame.iter_chunks() {
                for addr in chunk.columns()[0]
                    .as_any()
                    .downcast_ref::<BinaryArray<i64>>()
                    .unwrap()
                    .iter()
                {
                    addrs.push(Address::new(addr.unwrap()));
                }
            }

            let mut bloom = Bloom::random(addrs.len(), 0.000_001, 1000000);

            for addr in addrs {
                bloom.add(&addr);
            }

            bloom
        };

        let tx_addr_filter = {
            let mut path = data_path.to_owned();
            path.push(dir_name.to_string());
            path.push("tx.parquet");

            let lazy_frame =
                LazyFrame::scan_parquet(&path, Default::default()).map_err(Error::ScanParquet)?;
            let data_frame = lazy_frame
                .select(vec![col("dest")])
                .unique(None, UniqueKeepStrategy::First)
                .collect()
                .map_err(Error::ExecuteQuery)?;

            let mut addrs = Vec::new();

            for chunk in data_frame.iter_chunks() {
                for addr in chunk.columns()[0]
                    .as_any()
                    .downcast_ref::<BinaryArray<i64>>()
                    .unwrap()
                    .iter()
                {
                    if let Some(addr) = addr {
                        addrs.push(Address::new(addr));
                    }
                }
            }

            let mut bloom = Bloom::random(addrs.len(), 0.000_001, 1000000);

            for addr in addrs {
                bloom.add(&addr);
            }

            bloom
        };

        let parquet_idx = ParquetIdx {
            log_addr_filter,
            tx_addr_filter,
        };

        db.insert_parquet_idx(dir_name, &parquet_idx)
    }
}

struct Job {
    respond: oneshot::Sender<Result<()>>,
    kind: JobKind,
}

enum JobKind {
    WriteBatches((Vec<Vec<Block>>, Vec<Vec<Log>>)),
    RegisterParquetFolder(DirName),
}
