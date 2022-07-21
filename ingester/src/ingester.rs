use crate::config::{Config, IngestConfig};
use crate::db::DbHandle;
use crate::options::Options;
use crate::{Error, Result};
use eth_archive_core::eth_client::EthClient;
use eth_archive_core::eth_request::GetBlockByNumber;
use eth_archive_core::retry::Retry;
use eth_archive_core::types::Block;
use std::sync::Arc;
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
                return self
                    .eth_client
                    .send(GetBlockByNumber {
                        block_number: waiting_for,
                    })
                    .await
                    .map_err(Error::EthClient);
            } else {
                log::debug!("waiting for next block...");
                sleep(Duration::from_secs(1)).await;
            }
        }
    }

    async fn wait_and_insert_block(&self, block: Block) -> Result<()> {
        let block_number = block.number.0 as usize;

        loop {
            let min_block_number = self.db.get_min_block_number().await?.unwrap();
            if block_number - min_block_number <= self.cfg.block_window_size {
                self.db.insert_block(block).await?;
                return Ok(());
            } else {
                log::debug!("waiting for parquet writer to write tail...");
                sleep(Duration::from_secs(1)).await;
            }
        }
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
            let block = self.wait_for_next_block(block_number).await?;

            self.wait_and_insert_block(block).await?;

            block_number += 1;
        }
    }
}
