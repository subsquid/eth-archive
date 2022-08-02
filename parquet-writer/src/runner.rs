use crate::config::Config;
use crate::parquet_writer::ParquetWriter;
use crate::schema::{Blocks, Logs, Transactions};
use crate::{Error, Result};
use eth_archive_core::config::IngestConfig;
use eth_archive_core::db::DbHandle;
use eth_archive_core::eth_client::EthClient;
use eth_archive_core::eth_request::GetBlockByNumber;
use eth_archive_core::options::Options;
use eth_archive_core::retry::Retry;
use eth_archive_core::types::Block;
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
    _log_writer: ParquetWriter<Logs>,
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

        let eth_client =
            EthClient::new(config.ingest.eth_rpc_url.clone()).map_err(Error::CreateEthClient)?;
        let eth_client = Arc::new(eth_client);

        let (delete_tx, mut delete_rx) = mpsc::unbounded_channel();

        {
            let db = db.clone();
            tokio::spawn(async move {
                while let Some(block_number) = delete_rx.recv().await {
                    if block_number <= config.block_overlap_size {
                        continue;
                    }

                    let delete_up_to = block_number - config.block_overlap_size;

                    if let Err(e) = db.delete_blocks_up_to(delete_up_to as i64).await {
                        log::error!("failed to delete blocks up to {}:\n{}", delete_up_to, e);
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

        let block_writer = ParquetWriter::new(config.block, delete_tx.clone());
        let transaction_writer = ParquetWriter::new(config.transaction, delete_tx.clone());
        let log_writer = ParquetWriter::new(config.log, delete_tx);

        let retry = Retry::new(config.retry);

        Ok(Self {
            db,
            cfg: config.ingest,
            eth_client,
            block_writer,
            transaction_writer,
            _log_writer: log_writer,
            retry,
        })
    }

    async fn wait_for_next_blocks(&self, waiting_for: usize, step: usize) -> Result<Vec<Block>> {
        let start = waiting_for as i64;
        let end = (waiting_for + step) as i64;

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

            for block in blocks.iter() {
                let transactions = self
                    .db
                    .get_txs_of_block(block.number.0 as i64)
                    .await
                    .map_err(Error::GetTxsFromDb)?;

                self.transaction_writer.send(transactions).await;
            }

            self.block_writer.send(blocks).await;

            log::info!(
                "sent blocks {}-{} to writer",
                block_number,
                block_number + step
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

    async fn get_start_block(&self) -> Result<usize> {
        let mut dir = tokio::fs::read_dir(&self.block_writer.cfg.path)
            .await
            .map_err(Error::ReadParquetDir)?;
        let mut block_num = 0;
        while let Some(entry) = dir.next_entry().await.map_err(Error::ReadParquetDir)? {
            let file_name = entry
                .file_name()
                .into_string()
                .map_err(|_| Error::InvalidParquetFileName)?;
            let num = file_name.split('_').last().unwrap();
            let num = &num[..num.len() - ".parquet".len()];
            let num = num.parse::<usize>().unwrap() + 1;
            block_num = cmp::max(block_num, num);
        }

        Ok(block_num)
    }

    pub async fn initial_sync(&self) -> Result<usize> {
        let from_block = self.get_start_block().await?;

        let to_block = self.wait_for_start_block_number().await?;

        log::info!("starting initial sync up to: {}.", to_block);

        let step = self.cfg.http_req_concurrency * self.cfg.block_batch_size;
        for block_num in (from_block..to_block).step_by(step) {
            let concurrency = self.cfg.http_req_concurrency;
            let batch_size = self.cfg.block_batch_size;

            let batches = (0..concurrency)
                .map(|step_no| {
                    let start = block_num + step_no * batch_size;
                    let end = start + batch_size;
                    (start..end)
                        .map(|i| GetBlockByNumber { block_number: i })
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();

            let start_time = Instant::now();

            let batches = self
                .eth_client
                .clone()
                .send_batches(&batches, self.retry)
                .await
                .map_err(Error::EthClient)?;

            log::info!(
                "downloaded blocks {}-{} in {}ms",
                block_num,
                block_num + step,
                start_time.elapsed().as_millis()
            );

            for mut batch in batches {
                for block in batch.iter_mut() {
                    let transactions = mem::take(&mut block.transactions);
                    self.transaction_writer.send(transactions).await;
                }

                self.block_writer.send(batch).await;
            }
        }
        log::info!("finished initial sync up to block {}", to_block);
        Ok(to_block)
    }
}
