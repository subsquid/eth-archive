use crate::config::{Config, IngestConfig};
use crate::db::DbHandle;
use crate::options::Options;
use crate::{Error, Result};
use eth_archive_core::eth_client::EthClient;
use eth_archive_core::eth_request::GetBlockByNumber;
use eth_archive_core::retry::Retry;
use eth_archive_core::types::Block;
use std::sync::Arc;
use std::time::Instant;
use tokio::time::{sleep, Duration};

pub struct Ingester {
    db: Arc<DbHandle>,
    cfg: IngestConfig,
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
            cfg: config.ingest,
            eth_client,
            retry,
        })
    }

    async fn wait_for_next_block(&self, waiting_for: usize) -> Result<Block> {
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
                log::debug!("waiting for next block...");
                sleep(Duration::from_secs(1)).await;
            }
        }
    }

    async fn wait_and_insert_block(&self, block: Block) -> Result<()> {
        let block_number = block.number.0 as usize;

        loop {
            let min_block_number = self.db.get_min_block_number().await?;
            if min_block_number.is_none()
                || block_number - min_block_number.unwrap() <= self.cfg.block_window_size
            {
                self.db.insert_blocks(&[block]).await?;
                return Ok(());
            } else {
                log::debug!("waiting for parquet writer to write tail...");
                sleep(Duration::from_secs(1)).await;
            }
        }
    }

    async fn fast_sync(&self, from_block: usize, to_block: usize) -> Result<()> {
        log::info!("starting fast sync up to: {}.", to_block);

        let step = self.cfg.http_req_concurrency * self.cfg.block_batch_size;
        for block_num in (from_block..to_block).step_by(step) {
            log::info!("current block num is {}", block_num);
            let concurrency = self.cfg.http_req_concurrency;
            let batch_size = self.cfg.block_batch_size;
            let start_time = Instant::now();
            let group = (0..concurrency)
                .map(|step| {
                    let eth_client = self.eth_client.clone();
                    let start = block_num + step * batch_size;
                    let end = start + batch_size;
                    async move {
                        self.retry
                            .retry(move || {
                                let eth_client = eth_client.clone();
                                async move {
                                    let batch = (start..end)
                                        .map(|i| GetBlockByNumber { block_number: i })
                                        .collect::<Vec<_>>();
                                    eth_client
                                        .send_batch(&batch)
                                        .await
                                        .map_err(Error::EthClient)
                                }
                            })
                            .await
                    }
                })
                .collect::<Vec<_>>();
            let group = futures::future::join_all(group).await;
            log::info!(
                "downloaded {} blocks in {}ms",
                step,
                start_time.elapsed().as_millis()
            );
            let start_time = Instant::now();
            for batch in group {
                let batch = match batch {
                    Ok(batch) => batch,
                    Err(e) => {
                        log::error!("failed batch block req: {:#?}", e);
                        continue;
                    }
                };

                self.db.insert_blocks(&batch).await?;
            }
            log::info!(
                "inserted {} blocks in {}ms",
                step,
                start_time.elapsed().as_millis()
            );
        }

        log::info!("finished fast sync up to block {}", to_block);

        Ok(())
    }

    pub async fn run(&self) -> Result<()> {
        let mut block_number = match self.db.get_max_block_number().await? {
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

        log::info!("ingester starting from block {}", block_number);

        loop {
            let best_block = self
                .eth_client
                .get_best_block()
                .await
                .map_err(Error::EthClient)?;

            if block_number + self.cfg.block_window_size < best_block {
                self.fast_sync(block_number, best_block).await?;
                block_number = best_block;
                continue;
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
