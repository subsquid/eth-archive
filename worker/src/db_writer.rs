use crate::data_ctx::scan_parquet_args;
use crate::db::{DbHandle, Filter, ParquetIdx};
use crate::{Error, Result};
use eth_archive_core::dir_name::DirName;
use eth_archive_core::types::{Block, BlockRange, Log};
use polars::export::arrow::array::BinaryArray;
use polars::prelude::*;
use std::convert::TryFrom;
use std::hash::Hasher;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

pub struct DbWriter {
    tx: mpsc::Sender<Job>,
}

impl DbWriter {
    pub fn new(db: Arc<DbHandle>, data_path: &Option<PathBuf>) -> Self {
        let (tx, mut rx) = mpsc::channel::<Job>(4);

        let data_path = data_path.as_ref().map(|p| p.to_owned());

        std::thread::spawn(move || {
            while let Some(job) = rx.blocking_recv() {
                loop {
                    let res = match job.clone() {
                        Job::WriteBatches(batches) => db.insert_batches(batches),
                        Job::RegisterParquetFolder(dir_name) => {
                            Self::handle_register_parquet_folder(
                                &db,
                                data_path.as_ref().unwrap(),
                                dir_name,
                            )
                        }
                        Job::RunCompaction => {
                            db.compact();
                            break;
                        }
                    };

                    match res {
                        Ok(_) => break,
                        Err(e) => {
                            log::error!("failed to handle db write op:\n{}", e);
                            std::thread::sleep(Duration::from_secs(1));
                        }
                    }
                }
            }
        });

        Self { tx }
    }

    pub async fn write_batches(&self, batches: (Vec<BlockRange>, Vec<Vec<Block>>, Vec<Vec<Log>>)) {
        self.tx.send(Job::WriteBatches(batches)).await.ok().unwrap();
    }

    pub async fn register_parquet_folder(&self, dir_name: DirName) {
        self.tx
            .send(Job::RegisterParquetFolder(dir_name))
            .await
            .ok()
            .unwrap();
    }

    pub async fn run_compaction(&self) {
        self.tx.send(Job::RunCompaction).await.ok().unwrap();
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
                LazyFrame::scan_parquet(&path, scan_parquet_args()).map_err(Error::ScanParquet)?;
            let data_frame = lazy_frame
                .select(vec![col("address")])
                .unique(None, UniqueKeepStrategy::First)
                .collect()
                .map_err(Error::ExecuteQuery)?;

            filter_from_frame(data_frame)
        };

        let tx_addr_filter = {
            let mut path = data_path.to_owned();
            path.push(dir_name.to_string());
            path.push("tx.parquet");

            let lazy_frame =
                LazyFrame::scan_parquet(&path, scan_parquet_args()).map_err(Error::ScanParquet)?;
            let data_frame = lazy_frame
                .select(vec![col("dest")])
                .unique(None, UniqueKeepStrategy::First)
                .collect()
                .map_err(Error::ExecuteQuery)?;

            filter_from_frame(data_frame)
        };

        let parquet_idx = ParquetIdx {
            log_addr_filter,
            tx_addr_filter,
        };

        db.insert_parquet_idx(dir_name, &parquet_idx)?;

        db.delete_up_to(dir_name.range.to)?;

        Ok(())
    }
}

#[derive(Clone)]
enum Job {
    WriteBatches((Vec<BlockRange>, Vec<Vec<Block>>, Vec<Vec<Log>>)),
    RegisterParquetFolder(DirName),
    RunCompaction,
}

fn filter_from_frame(data_frame: DataFrame) -> Filter {
    let mut addrs = Vec::new();

    for chunk in data_frame.iter_chunks() {
        for addr in chunk.columns()[0]
            .as_any()
            .downcast_ref::<BinaryArray<i64>>()
            .unwrap()
            .iter()
            .flatten()
        {
            addrs.push(fnv1a(addr));
        }
    }

    Filter::try_from(&addrs).unwrap()
}

pub fn fnv1a(bytes: &[u8]) -> u64 {
    let mut hasher = fnv::FnvHasher::default();
    hasher.write(bytes);
    hasher.finish()
}
