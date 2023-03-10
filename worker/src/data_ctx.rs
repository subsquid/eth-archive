use crate::config::Config;
use crate::db::DbHandle;
use crate::db_writer::DbWriter;
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
use std::sync::atomic::{Ordering, AtomicUsize};
use std::collections::VecDeque;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::{cmp, io};

pub struct DataCtx {
    config: Config,
    db: Arc<DbHandle>,
    current_num_queries: AtomicUsize,
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

        Ok(Self {
            config,
            db,
            current_num_queries: AtomicUsize::new(0),
        })
    }

    pub async fn inclusive_height(&self) -> Result<Option<u32>> {
        Ok(match self.db.clone().height().await? {
            0 => None,
            num => Some(num - 1),
        })
    }

    pub async fn query(self: Arc<Self>, query: Query) -> Result<Vec<u8>> {
        let max_concurrent_queries = self.config.max_concurrent_queries.get();
        if self
            .current_num_queries
            .fetch_update(
                Ordering::SeqCst,
                Ordering::SeqCst,
                |current_num_queries: usize| {
                    if current_num_queries < max_concurrent_queries {
                        Some(current_num_queries + 1)
                    } else {
                        None
                    }
                },
            )
            .is_err()
        {
            return Err(Error::MaxNumberOfQueriesReached);
        }

        let res = self.clone().query_impl(query).await;

        self.current_num_queries.fetch_sub(1, Ordering::SeqCst);

        res
    }

    async fn query_impl(self: Arc<Self>, mut query: Query) -> Result<Vec<u8>> {
        // to_block is supposed to be inclusive in api but exclusive in implementation
        // so we add one to it here
        query.to_block = query.to_block.map(|a| a + 1);

        if let Some(to_block) = query.to_block {
            if query.from_block >= to_block {
                return Err(Error::InvalidBlockRange);
            }
        }

        if query.logs.is_empty() && query.transactions.is_empty() {
            return Err(Error::EmptyQuery);
        }

        let field_selection = query.field_selection();

        let serialize_task = SerializeTask::new(
            query.from_block,
            self.config.max_resp_body_size,
            self.inclusive_height().await?,
            field_selection,
        );

        if query.from_block >= self.db.clone().height().await? {
            return serialize_task.join().await;
        }

        let mut parquet_idxs = self.db.clone().iter_parquet_idxs(query.from_block, query.to_block).await?;

        while let Some(res) = parquet_idxs.recv().await {
            let (dir_name, parquet_idx) = res?;

            
        }

        serialize_task.join().await
    }
}

impl Query {
    fn field_selection(&self) -> FieldSelection {
        self.logs
            .iter()
            .map(|log| log.field_selection)
            .chain(self.transactions.iter().map(|tx| tx.field_selection))
            .fold(Default::default(), |a, b| a | b)
    }

    fn log_selection(&self) -> Vec<MiniLogSelection> {
        self.logs
            .iter()
            .map(|log| MiniLogSelection {
                address: log.address.clone(),
                topics: log.topics.clone(),
            })
            .collect()
    }

    fn tx_selection(&self) -> Vec<MiniTransactionSelection> {
        self.transactions
            .iter()
            .map(|transaction| MiniTransactionSelection {
                source: transaction.source.clone(),
                dest: transaction.dest.clone(),
                sighash: transaction.sighash.clone(),
                status: transaction.status,
            })
            .collect()
    }
}
