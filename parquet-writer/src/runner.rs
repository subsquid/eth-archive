use crate::config::IngestConfig;
use eth_archive_core::eth_client::EthClient;
use std::sync::Arc;

pub struct ParquetWriterRunner {
    cfg: IngestConfig,
    eth_client: Arc<EthClient>,
}
