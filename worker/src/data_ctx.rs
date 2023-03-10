use crate::config::Config;
use crate::db::DbHandle;
use crate::db_writer::{hash_addr, DbWriter};
use crate::downloader::Downloader;
use crate::field_selection::FieldSelection;
use crate::parquet_watcher::ParquetWatcher;
use crate::serialize_task::SerializeTask;
use crate::types::{MiniLogSelection, MiniQuery, MiniTransactionSelection, Query};
use crate::{Error, Result};
use arrayvec::ArrayVec;
use eth_archive_core::dir_name::DirName;
use eth_archive_core::ingest_metrics::IngestMetrics;
use eth_archive_core::retry::Retry;
use eth_archive_core::s3_client::{Direction, S3Client};
use eth_archive_core::types::{
    BlockRange, QueryResult, ResponseBlock, ResponseLog, ResponseRow, ResponseTransaction,
};
use futures::pin_mut;
use futures::stream::StreamExt;
use std::collections::VecDeque;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::{cmp, io};
use xorf::Filter;

pub struct DataCtx {
    config: Config,
    db: Arc<DbHandle>,
}

impl DataCtx {
    pub async fn new(config: Config, ingest_metrics: Arc<IngestMetrics>) -> Result<Self> {
        let db = DbHandle::new(&config.db_path, ingest_metrics.clone()).await?;
        let db = Arc::new(db);

        let db_writer = DbWriter::new(db.clone(), &config.data_path);
        let db_writer = Arc::new(db_writer);

        let retry = Retry::new(config.retry);

        Downloader {
            config: config.clone(),
            ingest_metrics: ingest_metrics.clone(),
            db: db.clone(),
            db_writer: db_writer.clone(),
            retry,
        }
        .spawn()
        .await?;

        if let Some(data_path) = &config.data_path {
            ParquetWatcher {
                db: db.clone(),
                data_path: data_path.to_owned(),
                db_writer: db_writer.clone(),
            }
            .spawn()
            .await?;
        }

        if let Some(data_path) = &config.data_path {
            if let Some(s3_config) = config.s3.into_parsed() {
                let s3_client = S3Client::new(retry, &s3_config)
                    .await
                    .map_err(Error::BuildS3Client)?;
                let s3_client = Arc::new(s3_client);

                s3_client.spawn_s3_sync(Direction::Down, data_path);
            } else {
                log::info!("no s3 config, disabling s3 sync");
            }
        }

        Ok(Self { config, db })
    }

    pub async fn inclusive_height(&self) -> Result<Option<u32>> {
        Ok(match self.db.clone().height().await? {
            0 => None,
            num => Some(num - 1),
        })
    }

    pub async fn query(self: Arc<Self>, query: Query) -> Result<Vec<u8>> {
        todo!();
    }
}
