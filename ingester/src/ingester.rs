use crate::config::Config;
use crate::schema::{Blocks, Logs, Transactions};
use crate::{Error, Result};
use eth_archive_core::eth_client::EthClient;
use eth_archive_core::dir_name::DirName;
use eth_archive_core::retry::Retry;
use eth_archive_core::types::BlockRange;
use futures::stream::StreamExt;
use itertools::Itertools;
use std::sync::Arc;
use std::cmp;
use std::path::PathBuf;
use futures::pin_mut;

pub struct Ingester {
    eth_client: Arc<EthClient>,
    data_path: PathBuf,
}

impl Ingester {
    pub async fn new(cfg: Config) -> Result<Self> {
        let retry = Retry::new(cfg.retry);
        let eth_client = EthClient::new(cfg.ingest, retry).map_err(Error::CreateEthClient)?;
        let eth_client = Arc::new(eth_client);
        let data_path = cfg.data_path;

        Ok(Self { eth_client, data_path })
    }

    pub async fn run(&self) -> Result<()> {
        log::info!("creating missing directories...");
        tokio::fs::create_dir_all(&self.data_path).await.map_err(Error::CreateMissingDirectories)?;

        let dir_names = DirName::list_sorted_folder_names(&self.data_path).await.map_err(Error::ListFolderNames)?;
        Self::delete_temp_dirs(&dir_names).await?;
        let block_num = Self::get_start_block(&dir_names);
        let best_block = self
            .eth_client
            .clone()
            .get_best_block()
            .await
            .map_err(Error::GetBestBlock)?;
        let batches = self.eth_client.clone().stream_batches(block_num, best_block);
        pin_mut!(batches);

        let mut data = ArrowData::default();
        let mut block_range = BlockRange::default();

        while let Some(batches) = batches.next().await {
            let (block_ranges, block_batches, log_batches) = batches.map_err(Error::GetBatch)?;
        }

        todo!()
    }

    async fn delete_temp_dirs(dir_names: &[DirName]) -> Result<()> {
        log::info!("deleting temporary directories...");

        for name in dir_names.iter() {
            if name.is_temp {
                tokio::fs::remove_dir_all(&name.to_string()).await.map_err(Error::RemoveTempDir)?;
            }
        }

        Ok(())
    }

    fn get_start_block(dir_names: &[DirName]) -> u32 {
        if dir_names.is_empty() {
            return 0;
        }
        let mut max = dir_names[0].range.to;
        for (a, b) in dir_names.iter().tuple_windows() {
            max = cmp::max(max, b.range.to);
        }

        max
    }
}

#[derive(Default)]
pub struct ArrowData {
    blocks: Blocks,
    txs: Transactions,
    logs: Logs,
}
