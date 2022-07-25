use crate::config::Config;
use crate::{Error, Result};
use eth_archive_core::db::DbHandle;
use eth_archive_core::eth_client::EthClient;
use eth_archive_core::eth_request::GetBlockByNumber;
use eth_archive_core::options::Options;
use eth_archive_core::retry::Retry;
use eth_archive_core::types::Block;
use std::sync::Arc;
use std::time::Instant;
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
        let config = tokio::fs::read_to_string(&options.cfg_path)
            .await
            .map_err(Error::ReadConfigFile)?;

        let config: Config = toml::de::from_str(&config).map_err(Error::ParseConfig)?;

        let db = DbHandle::new(options, &config.db)
            .await
            .map_err(|e| Error::CreateDbHandle(Box::new(e)))?;
        let db = Arc::new(db);

        let eth_client =
            EthClient::new(config.ingest.eth_rpc_url.clone()).map_err(Error::CreateEthClient)?;
        let eth_client = Arc::new(eth_client);

        let retry = Retry::new(config.retry);

        Ok(Self {
            db,
            cfg: config,
            eth_client,
            retry,
        })
    }

    async fn wait_for_next_block(&self, waiting_for: usize) -> Result<Block> {
        log::info!("waiting for next block...");

        loop {
            let block_number = self
                .eth_client
                .get_best_block()
                .await
                .map_err(Error::EthClient)?;

            if waiting_for < block_number {
                let block = self
                    .eth_client
                    .send(GetBlockByNumber {
                        block_number: waiting_for,
                    })
                    .await
                    .map_err(Error::EthClient)?;
                return Ok(block);
            } else {
                sleep(Duration::from_secs(1)).await;
            }
        }
    }

    async fn wait_and_insert_block(&self, block: Block) -> Result<()> {
        let block_number = block.number.0 as usize;

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
                    .insert_blocks(&[block])
                    .await
                    .map_err(Error::InsertBlocks)?;
                return Ok(());
            } else {
                log::debug!("waiting for parquet writer to write tail...");
                sleep(Duration::from_secs(1)).await;
            }
        }
    }

    async fn fast_sync(&self, from_block: usize, to_block: usize) -> Result<()> {
        log::info!("starting fast sync up to: {}.", to_block);

        let step = self.cfg.ingest.http_req_concurrency * self.cfg.ingest.block_batch_size;

        let (tx, mut rx) = mpsc::channel::<(Vec<Vec<Block>>, _, _)>(32);

        let write_task = tokio::spawn({
            let db = self.db.clone();
            let retry = self.retry;
            async move {
                while let Some((batches, from, to)) = rx.recv().await {
                    let start_time = Instant::now();

                    for batch in batches.iter() {
                        let db = db.clone();

                        retry
                            .retry(move || {
                                let db = db.clone();
                                async move {
                                    db.insert_blocks(batch).await.map_err(Error::InsertBlocks)
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

        for block_num in (from_block..to_block).step_by(step) {
            let concurrency = self.cfg.ingest.http_req_concurrency;
            let batch_size = self.cfg.ingest.block_batch_size;

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

            if tx
                .send((batches, block_num, block_num + step))
                .await
                .is_err()
            {
                log::error!("fast_sync: write_task stopped exiting download loop");
                break;
            }
        }

        log::info!("fast_sync: finished downloding, waiting for writing to end...");

        write_task.await.map_err(Error::JoinError)??;

        log::info!("finished fast sync up to block {}", to_block);

        Ok(())
    }

    pub async fn run(&self) -> Result<()> {
        let max_block_number = self
            .db
            .get_max_block_number()
            .await
            .map_err(Error::GetMaxBlockNumber)?;
        let mut block_number = match max_block_number {
            Some(block_number) => block_number + 1,
            None => {
                let block_number = self
                    .eth_client
                    .get_best_block()
                    .await
                    .map_err(Error::EthClient)?;
                if block_number <= self.cfg.block_window_size {
                    return Err(Error::BlockWindowBiggerThanBestblock);
                }

                block_number - self.cfg.block_window_size
            }
        };

        log::info!("starting to ingest from block {}", block_number);

        loop {
            let min_block_number = self
                .db
                .get_min_block_number()
                .await
                .map_err(Error::GetMinBlockNumber)?
                .unwrap_or(block_number);
            let max_block_number = block_number - 1;

            let step = self.cfg.ingest.http_req_concurrency * self.cfg.ingest.block_batch_size;
            if max_block_number + step < min_block_number + self.cfg.block_window_size {
                self.fast_sync(block_number, min_block_number + self.cfg.block_window_size)
                    .await?;

                block_number = min_block_number + self.cfg.block_window_size;
            }

            let block = self.wait_for_next_block(block_number).await?;

            self.wait_and_insert_block(block).await?;

            if block_number % 50 == 0 {
                log::info!("wrote block {}", block_number);
            }

            block_number += 1;
        }
    }
}
