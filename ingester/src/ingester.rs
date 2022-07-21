use crate::config::{Config, IngestConfig};
use crate::db::DbHandle;
use crate::options::Options;
use crate::{Error, Result};
use eth_archive_core::eth_client::EthClient;
use std::sync::Arc;

pub struct Ingester {
    db: Arc<DbHandle>,
    cfg: IngestConfig,
    eth_client: Arc<EthClient>,
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

        Ok(Self {
            db,
            cfg: config.ingest,
            eth_client,
        })
    }

    pub async fn run(&self) -> Result<()> {
        todo!();
    }
}
