use crate::config::Config;
use crate::options::Options;
use crate::schema::{Blocks, Logs, Transactions};
use crate::{Error, Result};
use eth_archive_core::config::IngestConfig;
use eth_archive_core::db::DbHandle;
use eth_archive_core::eth_client::EthClient;
use eth_archive_core::eth_request::{GetBlockByNumber, GetLogs};
use eth_archive_core::retry::Retry;
use eth_archive_core::types::Block;
use parquet_writer::{BlockRange, ParquetWriter};
use std::sync::Arc;
use std::time::Instant;
use std::{cmp, mem};
use tokio::fs;
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};

pub struct ParquetWriterRunner {
    db: Arc<DbHandle>,
    cfg: IngestConfig,
    eth_client: Arc<EthClient>,
    block_writer: ParquetWriter<Blocks>,
    transaction_writer: ParquetWriter<Transactions>,
    log_writer: ParquetWriter<Logs>,
    retry: Retry,
}

impl ParquetWriterRunner {
    pub async fn new(options: &Options) -> Result<Self> {
        let config = tokio::fs::read_to_string(
            options
                .cfg_path
                .as_deref()
                .unwrap_or("EthParquetWriter.toml"),
        )
        .await
        .map_err(Error::ReadConfigFile)?;

        let config: Config = toml::de::from_str(&config).map_err(Error::ParseConfig)?;

        let db = DbHandle::new(false, &config.db)
            .await
            .map_err(|e| Error::CreateDbHandle(Box::new(e)))?;
        let db = Arc::new(db);

        let eth_client = EthClient::new(&config.ingest).map_err(Error::CreateEthClient)?;
        let eth_client = Arc::new(eth_client);

        let (block_delete_tx, mut block_delete_rx) = mpsc::unbounded_channel();
        let (tx_delete_tx, mut tx_delete_rx) = mpsc::unbounded_channel();
        let (log_delete_tx, mut log_delete_rx) = mpsc::unbounded_channel();

        let retry = Retry::new(config.retry);

        {
            let db = db.clone();
            tokio::spawn(async move {
                let mut block_block_num = 0;
                let mut tx_block_num = 0;
                let mut log_block_num = 0;
                let mut prev_min = 0;

                loop {
                    let db = db.clone();

                    tokio::select! {
                        block_num = block_delete_rx.recv() => {
                            block_block_num = block_num.unwrap();
                        }
                        block_num = tx_delete_rx.recv() => {
                            tx_block_num = block_num.unwrap();
                        }
                        block_num = log_delete_rx.recv() => {
                            log_block_num = block_num.unwrap();
                        }
                    }

                    let new_min = cmp::min(cmp::min(block_block_num, tx_block_num), log_block_num);

                    if new_min != prev_min {
                        let block_num = new_min - config.block_overlap_size;
                        let delete_up_to = i64::try_from(block_num).unwrap();

                        let min = {
                            let db = db.clone();
                            retry
                                .retry(|| {
                                    let db = db.clone();
                                    async move { db.get_min_block_number().await }
                                })
                                .await
                                .unwrap()
                                .unwrap()
                        };

                        let delete_blocks = |db: Arc<DbHandle>, delete_up_to: i64| async move {
                            let res = retry
                                .retry(move || {
                                    let db = db.clone();
                                    async move { db.delete_blocks_up_to(delete_up_to).await }
                                })
                                .await;
                            if res.is_err() {
                                log::error!("failed to delete blocks up to {}", delete_up_to);
                            }
                        };

                        for num in (min..block_num)
                            .step_by(config.delete_blocks_chunk_size)
                            .skip(1)
                        {
                            delete_blocks(db.clone(), i64::try_from(num).unwrap()).await;
                        }
                        delete_blocks(db.clone(), delete_up_to).await;

                        prev_min = new_min;
                    }
                }
            });
        }

        if options.reset_data {
            log::info!("resetting parquet data");

            if let Err(e) = fs::remove_dir_all(&config.block.path).await {
                log::warn!("failed to remove block parquet directory:\n{}", e);
            }
            if let Err(e) = fs::remove_dir_all(&config.transaction.path).await {
                log::warn!("failed to remove transaction parquet directory:\n{}", e);
            }
            if let Err(e) = fs::remove_dir_all(&config.log.path).await {
                log::warn!("failed to remove log parquet directory:\n{}", e);
            }
        }

        let block_writer = ParquetWriter::new(config.block, block_delete_tx).await;
        let transaction_writer = ParquetWriter::new(config.transaction, tx_delete_tx).await;
        let log_writer = ParquetWriter::new(config.log, log_delete_tx).await;

        Ok(Self {
            db,
            cfg: config.ingest,
            eth_client,
            block_writer,
            transaction_writer,
            log_writer,
            retry,
        })
    }

    async fn wait_for_next_blocks(&self, waiting_for: usize, step: usize) -> Result<Vec<Block>> {
        let start = i64::try_from(waiting_for).unwrap();
        let end = i64::try_from(waiting_for + step).unwrap();

        log::info!("waiting for block {}", end);

        loop {
            let block = self
                .db
                .get_block(end)
                .await
                .map_err(Error::GetBlockFromDb)?;
            if block.is_some() {
                log::info!("getting blocks {}-{} from db", start, end);

                return self
                    .db
                    .get_blocks(start, end)
                    .await
                    .map_err(Error::GetBlocksFromDb);
            } else {
                log::debug!("waiting for next block...");
                sleep(Duration::from_secs(1)).await;
            }
        }
    }

    pub async fn run(&self) -> Result<()> {
        let mut block_number = self.initial_sync().await?;

        let step = self.cfg.http_req_concurrency * self.cfg.block_batch_size;

        loop {
            let blocks = self.wait_for_next_blocks(block_number, step).await?;

            let block_range = BlockRange {
                from: block_number,
                to: block_number + step,
            };

            self.block_writer.send((block_range, blocks)).await;

            const STEP: usize = 100;
            for from in (block_range.from..block_range.to).step_by(STEP) {
                let to = cmp::min(block_range.to, from + STEP);

                let block_range = BlockRange { from, to };

                let from = i64::try_from(from).unwrap();
                let to = i64::try_from(to).unwrap();

                let txs = self
                    .db
                    .get_txs(from, to)
                    .await
                    .map_err(Error::GetTxsFromDb)?;

                self.transaction_writer.send((block_range, txs)).await;

                let logs = self
                    .db
                    .get_logs(from, to)
                    .await
                    .map_err(Error::GetLogsFromDb)?;

                self.log_writer.send((block_range, logs)).await;
            }

            log::info!(
                "sent blocks {}-{} to writer",
                block_range.from,
                block_range.to,
            );

            block_number += step;
        }
    }

    async fn wait_for_start_block_number(&self) -> Result<usize> {
        loop {
            match self
                .db
                .get_min_block_number()
                .await
                .map_err(Error::GetMinBlockNumber)?
            {
                Some(min_num) => return Ok(min_num),
                None => {
                    log::info!("no blocks in database, waiting...");
                    sleep(Duration::from_secs(5)).await;
                }
            }
        }
    }

    pub async fn initial_sync(&self) -> Result<usize> {
        let from_block = cmp::min(
            cmp::min(
                self.block_writer.from_block,
                self.transaction_writer.from_block,
            ),
            self.log_writer.from_block,
        );

        let to_block = self.wait_for_start_block_number().await?;

        log::info!("starting initial sync up to: {}.", to_block);

        let step = self.cfg.http_req_concurrency * self.cfg.block_batch_size;
        for block_num in (from_block..to_block).step_by(step) {
            let concurrency = self.cfg.http_req_concurrency;
            let batch_size = self.cfg.block_batch_size;

            let block_batches = (0..concurrency)
                .filter_map(|step_no| {
                    let start = block_num + step_no * batch_size;
                    let end = cmp::min(start + batch_size, to_block);

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
                    let end = cmp::min(start + batch_size, to_block);

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

            log::info!(
                "starting to download blocks {}-{}",
                block_num,
                cmp::min(block_num + step, to_block),
            );

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
                cmp::min(block_num + step, to_block),
                start_time.elapsed().as_millis()
            );

            for (step_no, mut batch) in block_batches.into_iter().enumerate() {
                let start = block_num + step_no * batch_size;
                let end = cmp::min(start + batch_size, to_block);

                for block in batch.iter_mut() {
                    let transactions = mem::take(&mut block.transactions);

                    self.transaction_writer
                        .send((
                            BlockRange {
                                from: usize::try_from(block.number.0).unwrap(),
                                to: usize::try_from(block.number.0 + 1).unwrap(),
                            },
                            transactions,
                        ))
                        .await;
                }

                let block_range = BlockRange {
                    from: start,
                    to: end,
                };

                self.block_writer.send((block_range, batch)).await;
            }

            for (step_no, batch) in log_batches.into_iter().enumerate() {
                let start = block_num + step_no * batch_size;
                let end = cmp::min(start + batch_size, to_block);

                let block_range = BlockRange {
                    from: start,
                    to: end,
                };

                self.log_writer.send((block_range, batch)).await;
            }
        }
        log::info!("finished initial sync up to block {}", to_block);
        Ok(to_block)
    }
}
