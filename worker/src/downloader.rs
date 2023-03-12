use crate::config::Config;
use crate::db::DbHandle;
use crate::db_writer::DbWriter;
use crate::{Error, Result};
use eth_archive_core::eth_client::EthClient;
use eth_archive_core::ingest_metrics::IngestMetrics;
use eth_archive_core::retry::Retry;
use futures::StreamExt;
use std::sync::Arc;

pub struct Downloader {
    pub config: Config,
    pub ingest_metrics: Arc<IngestMetrics>,
    pub db: Arc<DbHandle>,
    pub db_writer: Arc<DbWriter>,
    pub retry: Retry,
}

impl Downloader {
    pub async fn spawn(self) -> Result<()> {
        let eth_client =
            EthClient::new(self.config.ingest.clone(), self.retry, self.ingest_metrics)
                .map_err(Error::CreateEthClient)?;
        let eth_client = Arc::new(eth_client);

        let db_height = self.db.db_height();
        let parquet_height = self.db.parquet_height();

        let start_height = if db_height == 0 {
            parquet_height
        } else {
            db_height
        };

        let best_block = eth_client
            .clone()
            .get_best_block()
            .await
            .map_err(Error::GetBestBlock)?;

        let initial_hot_block_range = self.config.initial_hot_block_range;

        let db_writer = self.db_writer;

        tokio::spawn(async move {
            let mut start = match initial_hot_block_range {
                Some(range) if best_block > range => best_block - range,
                _ => 0,
            };

            if start_height > start {
                start = start_height;
            }

            let batches = eth_client.stream_batches(Some(start), None);
            futures::pin_mut!(batches);

            while let Some(res) = batches.next().await {
                let data = res.unwrap();
                db_writer.write_batches(data).await;
            }
        });

        Ok(())
    }
}
