use crate::archive_client::ArchiveClient;
use crate::config::Config;
use crate::{Error, Result};
use eth_archive_core::eth_client::EthClient;
use eth_archive_core::ingest_metrics::IngestMetrics;
use eth_archive_core::retry::Retry;
use std::sync::Arc;

pub struct Verifier {
    archive_client: Arc<ArchiveClient>,
    eth_client: Arc<EthClient>,
}

impl Verifier {
    pub async fn new(config: Config) -> Result<Self> {
        let retry = Retry::new(config.retry);
        let metrics = IngestMetrics::new();
        let metrics = Arc::new(metrics);
        let eth_client = EthClient::new(config.ingest.clone(), retry, metrics.clone())
            .map_err(Error::CreateEthClient)?;
        let eth_client = Arc::new(eth_client);

        let archive_client = ArchiveClient::new(config.archive_url, config.ingest, retry)
            .map_err(|e| Error::CreateArchiveClient(Box::new(e)))?;
        let archive_client = Arc::new(archive_client);

        Ok(Self {
            archive_client,
            eth_client,
        })
    }

    pub async fn run(&self) -> Result<()> {
        todo!()
    }
}
