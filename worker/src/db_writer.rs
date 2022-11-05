use crate::db::DbHandle;
use crate::{Error, Result};
use eth_archive_core::dir_name::DirName;
use eth_archive_core::types::{Block, Log};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

pub struct DbWriter {
    tx: mpsc::Sender<Job>,
}

impl DbWriter {
    pub fn new(db: Arc<DbHandle>, data_path: &Path) -> Self {
        let (tx, mut rx) = mpsc::channel::<Job>(2);

        std::thread::spawn(move || {
            while let Some(job) = rx.blocking_recv() {
                let res = match job.kind {
                    JobKind::WriteBatches(batches) => Self::handle_write_batches(&db, batches),
                    JobKind::RegisterParquetFolder(dir_name) => {
                        Self::handle_register_parquet_folder(&db, dir_name)
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
    ) -> Result<()> {
        todo!()
    }

    fn handle_register_parquet_folder(db: &DbHandle, dir_name: DirName) -> Result<()> {
        todo!()
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
