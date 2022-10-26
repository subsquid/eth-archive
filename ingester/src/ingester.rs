use crate::config::Config;
use crate::schema::{Blocks, Logs, Transactions};
use crate::{Error, Result};
use eth_archive_core::config::IngestConfig;
use eth_archive_core::db::DbHandle;
use eth_archive_core::eth_client::EthClient;
use eth_archive_core::eth_request::{GetBlockByNumber, GetLogs};
use eth_archive_core::retry::Retry;
use eth_archive_core::types::{Block, BlockRange};
use eth_archive_parquet_writer::{ParquetConfig, ParquetWriter};
use std::sync::Arc;
use std::time::Instant;
use std::{cmp, mem};
use tokio::fs;
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};
use eth_archive_core::ingest_stream::IngestClient;

pub struct Ingester {
    cfg: IngestConfig,
    eth_client: Arc<EthClient>,
    block_writer: ParquetWriter<Blocks>,
    transaction_writer: ParquetWriter<Transactions>,
    log_writer: ParquetWriter<Logs>,
    retry: Retry,
}

impl Ingester {
    pub async fn new(config: Config) -> Result<Self> {
        let eth_client = EthClient::new(&config.ingest).map_err(Error::CreateEthClient)?;
        let eth_client = Arc::new(eth_client);

        let retry = Retry::new(config.retry);

        let block_config = ParquetConfig {
            name: "block".to_string(),
            path: config.data_path.join("block"),
            items_per_file: 32768,
            items_per_row_group: 512,
            channel_size: 1024,
        };
        let transaction_config = ParquetConfig {
            name: "tx".to_string(),
            path: config.data_path.join("tx"),
            items_per_file: 65536,
            items_per_row_group: 512,
            channel_size: 3072,
        };
        let log_config = ParquetConfig {
            name: "log".to_string(),
            path: config.data_path.join("log"),
            items_per_file: 65536,
            items_per_row_group: 512,
            channel_size: 2048,
        };

        if config.reset_data {
            log::info!("resetting parquet data");

            if let Err(e) = fs::remove_dir_all(&block_config.path).await {
                log::warn!("failed to remove block parquet directory:\n{}", e);
            }
            if let Err(e) = fs::remove_dir_all(&transaction_config.path).await {
                log::warn!("failed to remove transaction parquet directory:\n{}", e);
            }
            if let Err(e) = fs::remove_dir_all(&log_config.path).await {
                log::warn!("failed to remove log parquet directory:\n{}", e);
            }
        }

        let block_writer = ParquetWriter::new(block_config, block_delete_tx).await;
        let transaction_writer = ParquetWriter::new(transaction_config, tx_delete_tx).await;
        let log_writer = ParquetWriter::new(log_config, log_delete_tx).await;

        Ok(Self {
            cfg: config.ingest,
            eth_client,
            block_writer,
            transaction_writer,
            log_writer,
            retry,
        })
    }

    pub async fn run(&self) -> Result<()> {

    }
}
