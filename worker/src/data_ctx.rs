use crate::config::Config;
use crate::db::DbHandle;
use crate::db_writer::DbWriter;
use crate::downloader::Downloader;
use crate::field_selection::FieldSelection;
use crate::parquet_query::ParquetQuery;
use crate::parquet_watcher::ParquetWatcher;
use crate::serialize_task::SerializeTask;
use crate::types::{MiniLogSelection, MiniQuery, MiniTransactionSelection, Query};
use crate::{Error, Result};
use eth_archive_core::ingest_metrics::IngestMetrics;
use eth_archive_core::rayon_async;
use eth_archive_core::retry::Retry;
use eth_archive_core::s3_client::{Direction, S3Client};
use eth_archive_core::types::{BlockRange, QueryResult};
use std::cmp;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use xorf::BinaryFuse8;

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

    async fn query_impl(self: Arc<Self>, query: Query) -> Result<Vec<u8>> {
        let mut query = query;
        // to_block is supposed to be inclusive in api but exclusive in implementation
        // so we add one to it here
        query.to_block = query.to_block.map(|a| a + 1);
        let query = query;

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

        let field_selection = field_selection.with_join_columns();

        let parquet_height = self.db.clone().parquet_height().await?;

        if query.from_block < parquet_height {
            self.parquet_query(&serialize_task, &query, field_selection)
                .await?;
        }

        if serialize_task.is_closed() {
            return serialize_task.join().await;
        }

        let from_block = cmp::max(query.from_block, parquet_height);
        self.hot_data_query(from_block, &serialize_task, &query, field_selection)
            .await?;

        serialize_task.join().await
    }

    async fn parquet_query(
        &self,
        serialize_task: &SerializeTask,
        query: &Query,
        field_selection: FieldSelection,
    ) -> Result<()> {
        let mut parquet_idxs = self
            .db
            .clone()
            .iter_parquet_idxs(query.from_block, query.to_block)
            .await?;

        let concurrency = self.config.max_parquet_query_concurrency.get();
        let mut jobs: VecDeque<futures::channel::oneshot::Receiver<_>> =
            VecDeque::with_capacity(concurrency);

        while let Some(res) = parquet_idxs.recv().await {
            let (dir_name, parquet_idx) = res?;

            if jobs.len() == concurrency {
                let job = jobs.pop_front().unwrap();

                let (res, block_range) = job.await.unwrap();

                let res = res?;

                if !serialize_task.send((res, block_range)) {
                    break;
                }
            }

            let from_block = cmp::max(dir_name.range.from, query.from_block);
            let to_block = match query.to_block {
                Some(to_block) => cmp::min(dir_name.range.to, to_block),
                None => dir_name.range.to,
            };

            let block_range = BlockRange {
                from: from_block,
                to: to_block,
            };

            let (logs, transactions) = rayon_async::spawn({
                let query = query.clone();
                move || {
                    (
                        query.pruned_log_selection(&parquet_idx),
                        query.pruned_tx_selection(&parquet_idx),
                    )
                }
            })
            .await;

            let mini_query = MiniQuery {
                from_block,
                to_block,
                logs,
                transactions,
                field_selection,
                include_all_blocks: query.include_all_blocks,
            };

            let (tx, rx) = futures::channel::oneshot::channel();

            // don't spawn a thread if there is nothing to query
            if !mini_query.include_all_blocks
                && mini_query.logs.is_empty()
                && mini_query.transactions.is_empty()
            {
                tx.send((Ok(QueryResult { data: Vec::new() }), block_range))
                    .ok();
            } else {
                tokio::spawn(async move {
                    let res = ParquetQuery {}.run().await;

                    tx.send((res, block_range)).ok();
                });
            }

            jobs.push_back(rx);
        }

        while let Some(job) = jobs.pop_front() {
            let (res, block_range) = job.await.unwrap();

            let res = res?;

            if !serialize_task.send((res, block_range)) {
                break;
            }
        }

        Ok(())
    }

    async fn hot_data_query(
        &self,
        from_block: u32,
        serialize_task: &SerializeTask,
        query: &Query,
        field_selection: FieldSelection,
    ) -> Result<()> {
        let archive_height = self.db.clone().height().await?;

        if from_block >= archive_height {
            return Ok(());
        }

        let to_block = match query.to_block {
            Some(to_block) => cmp::min(archive_height, to_block),
            None => archive_height,
        };

        let step = usize::try_from(self.config.db_query_batch_size).unwrap();
        for start in (from_block..to_block).step_by(step) {
            let end = cmp::min(to_block, start + self.config.db_query_batch_size);

            let mini_query = MiniQuery {
                from_block: start,
                to_block: end,
                logs: query.log_selection(),
                transactions: query.tx_selection(),
                field_selection,
                include_all_blocks: query.include_all_blocks,
            };

            if serialize_task.is_closed() {
                break;
            }

            let res = self.db.clone().query(mini_query).await?;

            let block_range = BlockRange {
                from: start,
                to: end,
            };

            if !serialize_task.send((res, block_range)) {
                break;
            }
        }

        Ok(())
    }
}

impl FieldSelection {
    fn with_join_columns(mut self) -> Self {
        self.block.number = true;
        self.transaction.hash = true;
        self.transaction.block_number = true;
        self.transaction.transaction_index = true;
        self.transaction.dest = true;
        self.transaction.source = true;
        self.transaction.status = true;
        self.log.block_number = true;
        self.log.log_index = true;
        self.log.transaction_index = true;
        self.log.address = true;
        self.log.topics = true;

        self
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

    fn pruned_log_selection(&self, _parquet_idx: &BinaryFuse8) -> Vec<MiniLogSelection> {
        todo!()
    }

    fn pruned_tx_selection(&self, _parquet_idx: &BinaryFuse8) -> Vec<MiniTransactionSelection> {
        todo!()
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
