use crate::archive_client::ArchiveClient;
use crate::config::Config;
use crate::{Error, Result};
use eth_archive_core::eth_client::EthClient;
use eth_archive_core::ingest_metrics::IngestMetrics;
use eth_archive_core::retry::Retry;
use eth_archive_worker::Query as ArchiveQuery;
use futures::pin_mut;
use futures::stream::StreamExt;
use std::sync::Arc;

pub struct Verifier {
    archive_client: Arc<ArchiveClient>,
    eth_client: Arc<EthClient>,
    config: Config,
}

impl Verifier {
    pub async fn new(config: Config) -> Result<Arc<Self>> {
        let retry = Retry::new(config.retry);
        let metrics = IngestMetrics::new();
        let metrics = Arc::new(metrics);
        let eth_client = EthClient::new(config.ingest.clone(), retry, metrics.clone())
            .map_err(Error::CreateEthClient)?;
        let eth_client = Arc::new(eth_client);

        let archive_client =
            ArchiveClient::new(config.archive_url.clone(), config.ingest.clone(), retry)
                .map_err(|e| Error::CreateArchiveClient(Box::new(e)))?;
        let archive_client = Arc::new(archive_client);

        Ok(Self {
            archive_client,
            eth_client,
            config,
        })
    }

    pub async fn run(self: Arc<Self>) -> Result<()> {
        let start = 0;
        let step = self.config.skip.get();
        let step = usize::try_from(step).unwrap();

        for block_num in (start..).step_by(step) {
            let futs = self.config.offsets.iter().map(|| {
                let verifier = self.clone();
            });

            for offset in self.config.offsets.iter() {}

            let batches = self.eth_client.clone().stream_batches(
                Some(0),
                None,
                self.config.skip.map(|s| s.get()),
            );
            pin_mut!(batches);

            let batch = batches.next().await.unwrap();
            let (block_ranges, block_batches, log_batches) = batches.map_err(Error::GetBatch)?;

            // get all of the fields so we can compare data more conveniently
            let field_selection = !FieldSelection::default();

            let mut query_range = BlockRange {
                from: block_num,
                to: block_num,
            };
            let mut logs = Vec::new();
            let mut transactions = Vec::new();

            for ((block_range, block_batch), log_batch) in block_ranges
                .into_iter()
                .zip(block_batches.into_iter())
                .zip(log_batches.into_iter())
            {
                query_range += block_range;
            }

            let mut archive_query = ArchiveQuery {
                from_block: 0,
                to_block: Some(0),
                logs: Vec::new(),
                transactions: Vec::new(),
            };
        }
    }
}
