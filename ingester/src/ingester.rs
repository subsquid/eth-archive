use crate::config::Config;
use crate::options::Options;
use crate::{Error, Result};
use eth_archive_core::db::DbHandle;
use eth_archive_core::eth_client::EthClient;
use eth_archive_core::eth_request::{GetBlockByNumber, GetLogs};
use eth_archive_core::retry::Retry;
use eth_archive_core::types::{Block, Log};
use std::sync::Arc;
use std::time::Instant;
use std::{cmp, mem};
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};

pub struct Ingester {
    db: Arc<DbHandle>,
    cfg: Config,
    eth_client: Arc<EthClient>,
    retry: Retry,
}

impl Ingester {
    pub async fn new(options: &Options) -> Result<Self> {
        let config =
            tokio::fs::read_to_string(options.cfg_path.as_deref().unwrap_or("EthIngester.toml"))
                .await
                .map_err(Error::ReadConfigFile)?;

        let config: Config = toml::de::from_str(&config).map_err(Error::ParseConfig)?;

        let db = DbHandle::new(options.reset_data, &config.db)
            .await
            .map_err(|e| Error::CreateDbHandle(Box::new(e)))?;
        let db = Arc::new(db);

        let eth_client = EthClient::new(&config.ingest).map_err(Error::CreateEthClient)?;
        let eth_client = Arc::new(eth_client);

        let retry = Retry::new(config.retry);

        Ok(Self {
            db,
            cfg: config,
            eth_client,
            retry,
        })
    }

    async fn get_best_block(&self) -> Result<usize> {
        let num = self
            .eth_client
            .get_best_block()
            .await
            .map_err(Error::GetBestBlock)?;

        let offset = 20;

        Ok(if num < offset { 0 } else { num - offset })
    }

    async fn wait_for_next_block(&self, waiting_for: usize) -> Result<(Block, Vec<Log>)> {
        log::info!("waiting for block {}", waiting_for);

        loop {
            let block_number = self.get_best_block().await?;

            if waiting_for < block_number {
                let block = self
                    .eth_client
                    .send(GetBlockByNumber {
                        block_number: waiting_for,
                    })
                    .await
                    .map_err(Error::EthClient)?;

                let logs = self
                    .eth_client
                    .send(GetLogs {
                        from_block: waiting_for,
                        to_block: waiting_for,
                    })
                    .await
                    .map_err(Error::EthClient)?;

                return Ok((block, logs));
            } else {
                sleep(Duration::from_secs(1)).await;
            }
        }
    }

    async fn wait_and_insert_block(&self, block: Block, logs: Vec<Log>) -> Result<()> {
        log::info!("waiting for parquet writer to delete tail...");

        let block_number = usize::try_from(block.number.0).unwrap();

        loop {
            let min_block_number = self
                .db
                .get_min_block_number()
                .await
                .map_err(Error::GetMinBlockNumber)?;
            if min_block_number.is_none()
                || block_number - min_block_number.unwrap() <= self.cfg.block_window_size
            {
                self.db
                    .insert_blocks_data(&[block], &logs)
                    .await
                    .map_err(Error::InsertBlocks)?;

                log::info!("wrote block {}", block_number);

                return Ok(());
            } else {
                sleep(Duration::from_secs(1)).await;
            }
        }
    }

    async fn fast_sync(&self, from_block: usize, to_block: usize) -> Result<()> {
        log::info!("starting fast sync up to: {}.", to_block);

        let step = self.cfg.ingest.http_req_concurrency * self.cfg.ingest.block_batch_size;

        let (tx, mut rx) = mpsc::channel::<(Vec<Vec<Block>>, Vec<Vec<Log>>, _, _)>(2);

        let write_task = tokio::spawn({
            let db = self.db.clone();
            let retry = self.retry;
            async move {
                while let Some((block_batches, log_batches, from, to)) = rx.recv().await {
                    let start_time = Instant::now();

                    for (blocks, logs) in block_batches.iter().zip(log_batches.iter()) {
                        let db = db.clone();
                        retry
                            .retry(move || {
                                let db = db.clone();
                                async move {
                                    db.insert_blocks_data(blocks, logs)
                                        .await
                                        .map_err(Error::InsertBlocks)
                                }
                            })
                            .await
                            .map_err(Error::Retry)?;
                    }

                    log::info!(
                        "inserted blocks {}-{} in {}ms",
                        from,
                        to,
                        start_time.elapsed().as_millis()
                    );
                }

                Ok(())
            }
        });

        let best_block = self.get_best_block().await?;

        for block_num in (from_block..to_block).step_by(step) {
            let concurrency = self.cfg.ingest.http_req_concurrency;
            let batch_size = self.cfg.ingest.block_batch_size;

            let block_batches = (0..concurrency)
                .filter_map(|step_no| {
                    let start = block_num + step_no * batch_size;
                    let end = cmp::min(start + batch_size, best_block);

                    let batch = (start..end)
                        .map(|i| GetBlockByNumber { block_number: i })
                        .collect::<Vec<_>>();

                    if batch.is_empty() {
                        None
                    } else {
                        Some(batch)
                    }
                })
                .collect::<Vec<_>>();

            let log_batches = (0..concurrency)
                .filter_map(|step_no| {
                    let start = block_num + step_no * batch_size;
                    let end = cmp::min(start + batch_size, best_block);

                    if start < end {
                        Some(GetLogs {
                            from_block: start,
                            to_block: end - 1,
                        })
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();

            let start_time = Instant::now();

            let block_batches = self
                .eth_client
                .clone()
                .send_batches(&block_batches, self.retry)
                .await
                .map_err(Error::EthClient)?;
            let log_batches = self
                .eth_client
                .clone()
                .send_concurrent(&log_batches, self.retry)
                .await
                .map_err(Error::EthClient)?;

            log::info!(
                "downloaded blocks {}-{} in {}ms",
                block_num,
                block_num + step,
                start_time.elapsed().as_millis()
            );

            if tx
                .send((block_batches, log_batches, block_num, block_num + step))
                .await
                .is_err()
            {
                panic!("fast_sync: write_task stopped prematurely.");
            }
        }

        mem::drop(tx);

        log::info!("fast_sync: finished downloading, waiting for writing to end...");

        write_task.await.map_err(Error::JoinError)??;

        log::info!("finished fast sync up to block {}", to_block);

        Ok(())
    }

    async fn get_start_block(&self) -> Result<usize> {
        let max_block_number = self
            .db
            .get_max_block_number()
            .await
            .map_err(Error::GetMaxBlockNumber)?;
        match max_block_number {
            Some(block_number) => Ok(block_number + 1),
            None => {
                let block_number = self.get_best_block().await?;
                if block_number <= self.cfg.block_window_size {
                    return Err(Error::BlockWindowBiggerThanBestblock);
                }

                Ok(block_number - self.cfg.block_window_size)
            }
        }
    }

    pub async fn run(&self) -> Result<()> {
        let mut block_number = self.get_start_block().await?;

        log::info!("starting to ingest from block {}", block_number);

        loop {
            let min_block_number = self
                .db
                .get_min_block_number()
                .await
                .map_err(Error::GetMinBlockNumber)?
                .unwrap_or(block_number);
            let max_block_number = block_number - 1;

            let best_block = self.get_best_block().await?;

            let step = self.cfg.ingest.http_req_concurrency * self.cfg.ingest.block_batch_size;
            if max_block_number + step
                < cmp::min(min_block_number + self.cfg.block_window_size, best_block)
            {
                self.fast_sync(block_number, min_block_number + self.cfg.block_window_size)
                    .await?;

                block_number = self
                    .db
                    .get_max_block_number()
                    .await
                    .map_err(Error::GetMaxBlockNumber)?
                    .unwrap()
                    + 1;
            }

            let (block, logs) = self.wait_for_next_block(block_number).await?;

            self.wait_and_insert_block(block, logs).await?;

            block_number += 1;
        }
    }
}
