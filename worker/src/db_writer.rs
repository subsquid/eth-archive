use crate::db::{DbHandle, ParquetIdx};
use crate::{Error, Result};
use eth_archive_core::dir_name::DirName;
use eth_archive_core::types::{Block, BlockRange, Log};
use std::collections::HashSet;
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

    #[allow(clippy::manual_flatten)]
    fn handle_register_parquet_folder(
        db: &DbHandle,
        data_path: &Path,
        dir_name: DirName,
    ) -> Result<()> {
        todo!();
    }
}

#[derive(Clone)]
enum Job {
    WriteBatches((Vec<BlockRange>, Vec<Vec<Block>>, Vec<Vec<Log>>)),
    RegisterParquetFolder(DirName),
}

pub fn hash_addr(addr: &[u8]) -> u64 {
    xxhash_rust::xxh3::xxh3_64(addr)
}
