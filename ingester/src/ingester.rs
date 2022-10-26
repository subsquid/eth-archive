use crate::config::Config;
use crate::schema::{Blocks, Logs, Transactions};
use crate::{Error, Result};
use eth_archive_core::config::IngestConfig;
use eth_archive_core::eth_client::EthClient;
use eth_archive_core::eth_request::{GetBlockByNumber, GetLogs};
use eth_archive_core::ingest_stream::IngestClient;
use eth_archive_core::retry::Retry;
use eth_archive_core::types::{Block, BlockRange};
use std::sync::Arc;
use std::time::Instant;
use std::{cmp, mem};
use tokio::fs;
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};

pub struct Ingester {
    eth_client: Arc<EthClient>,
}

impl Ingester {
    pub async fn new(config: Config) -> Result<Self> {
        let retry = Retry::new(config.retry);
        let eth_client = EthClient::new(config.ingest, retry).map_err(Error::CreateEthClient)?;
        let eth_client = Arc::new(eth_client);

        Ok(Self {
            eth_client,
        })
    }

    pub async fn run(&self) -> Result<()> {

    }
}
