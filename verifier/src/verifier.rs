#[allow(unused)]
use crate::archive_client::ArchiveClient;
use crate::config::Config;
use crate::{Error, Result};
use eth_archive_core::eth_client::EthClient;
use eth_archive_core::ingest_metrics::IngestMetrics;
use eth_archive_core::retry::Retry;
use eth_archive_worker::FieldSelection;
use eth_archive_worker::Query as ArchiveQuery;
use futures::pin_mut;
use futures::stream::StreamExt;
use std::sync::Arc;
use std::time::Duration;

pub struct Verifier {
    archive_client: Arc<ArchiveClient>,
    eth_client: Arc<EthClient>,
    config: Config,
    metrics: Arc<IngestMetrics>,
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

        Ok(Arc::new(Self {
            archive_client,
            eth_client,
            config,
            metrics,
        }))
    }

    async fn execute_point(self: Arc<Self>, _block_num: u32) -> Result<()> {
        todo!()
        // let batches = self.eth_client.clone().stream_batches(
        //     Some(block_num),
        //     None,
        //     self.config.skip.map(|s| s.get()),
        // );
        // pin_mut!(batches);

        // for _ in 0..self.config.batches_per_step {
        //     let batch = batches.next().await.unwrap();
        //     let (block_ranges, block_batches, log_batches) = batch.map_err(Error::GetBatch)?;

        // }

        // // get all of the fields so we can compare data more conveniently
        // let field_selection = !FieldSelection::default();

        // let mut query_range = BlockRange {
        //     from: block_num,
        //     to: block_num,
        // };
        // let mut logs = Vec::new();
        // let mut transactions = Vec::new();

        // for ((block_range, block_batch), log_batch) in block_ranges
        //     .into_iter()
        //     .zip(block_batches.into_iter())
        //     .zip(log_batches.into_iter())
        // {
        //     query_range += block_range;
        // }

        // let mut archive_query = ArchiveQuery {
        //     from_block: 0,
        //     to_block: Some(0),
        //     logs: Vec::new(),
        //     transactions: Vec::new(),
        // };

        // Ok(())
    }

    pub async fn run(self: Arc<Self>) -> Result<()> {
        let start = 0u32;
        let step = self.config.step.get();
        let step = usize::try_from(step).unwrap();

        let mut block_num = start;
        let height = loop {
            let height = self
                .archive_client
                .get_height()
                .await
                .map_err(|e| Error::GetArchiveHeight(Box::new(e)))?;

            match height {
                Some(height) => break height,
                None => tokio::time::sleep(Duration::from_secs(5)).await,
            }
        };

        for block_num in (start..).step_by(step) {
            loop {
                let height = self
                    .archive_client
                    .get_height()
                    .await
                    .map_err(|e| Error::GetArchiveHeight(Box::new(e)))?;

                match height {
                    Some(height) if height >= block_num => break,
                    _ => tokio::time::sleep(Duration::from_secs(5)).await,
                }
            }

            let futs = self.config.offsets.iter().filter_map(|&offset| {
                block_num
                    .checked_sub(offset)
                    .map(|b| self.clone().execute_point(b))
            });

            futures::future::try_join_all(futs).await?;

            self.metrics.record_write_height(block_num);
        }

        Ok(())
    }
}
