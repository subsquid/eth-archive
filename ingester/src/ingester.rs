use crate::config::{Config, IngestConfig};
use crate::db::DbHandle;
use crate::options::Options;
use crate::{Error, Result};
use eth_archive_core::eth_client::EthClient;
use eth_archive_core::eth_request::GetBlockByNumber;
use eth_archive_core::retry::Retry;
use std::sync::Arc;

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

    pub async fn run(&self) -> Result<()> {
        let block_number = match self.db.get_max_block_number().await? {
            Some(block_number) => block_number,
            None => {
                let block_number = self
                    .eth_client
                    .get_best_block()
                    .await
                    .map_err(Error::EthClient)?;
                if block_number >= self.cfg.block_window_size {
                    return Err(Error::BlockWindowBiggerThanBestblock);
                }
                block_number - self.cfg.block_window_size
            }
        };

        loop {
            let block = self
                .retry
                .retry(move || {
                    let eth_client = self.eth_client.clone();
                    async move {
                        eth_client
                            .send(GetBlockByNumber { block_number })
                            .await
                            .map_err(Error::EthClient)
                    }
                })
                .await
                .map_err(Error::GetBlock)?;

            self.db.insert_block(block).await?;

            todo!();
        }
    }
}
