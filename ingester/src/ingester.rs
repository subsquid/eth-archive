use crate::config::Config;
use crate::consts::MAX_PENDING_FILE_WRITES;
use crate::schema::{Blocks, Logs, Transactions};
use crate::{Error, Result};
use eth_archive_core::dir_name::DirName;
use eth_archive_core::eth_client::EthClient;
use eth_archive_core::retry::Retry;
use eth_archive_core::types::BlockRange;
use futures::pin_mut;
use futures::stream::StreamExt;
use itertools::Itertools;
use std::path::Path;
use std::sync::Arc;
use std::{cmp, mem};
use tokio::sync::mpsc;

pub struct Ingester {
    eth_client: Arc<EthClient>,
    cfg: Config,
}

impl Ingester {
    pub async fn new(cfg: Config) -> Result<Self> {
        let retry = Retry::new(cfg.retry);
        let eth_client =
            EthClient::new(cfg.ingest.clone(), retry).map_err(Error::CreateEthClient)?;
        let eth_client = Arc::new(eth_client);

        Ok(Self { eth_client, cfg })
    }

    pub async fn run(&self) -> Result<()> {
        log::info!("creating missing directories...");
        let data_path = &self.cfg.data_path;

        tokio::fs::create_dir_all(data_path)
            .await
            .map_err(Error::CreateMissingDirectories)?;

        let dir_names = DirName::list_sorted_folder_names(data_path)
            .await
            .map_err(Error::ListFolderNames)?;
        Self::delete_temp_dirs(&dir_names).await?;

        let block_num = Self::get_start_block(&dir_names)?;

        let batches = self
            .eth_client
            .clone()
            .stream_batches(Some(block_num), None);
        pin_mut!(batches);

        let mut data = Data::default();

        let (sender, mut receiver): (mpsc::Sender<Data>, _) =
            mpsc::channel(MAX_PENDING_FILE_WRITES);

        let data_path = self.cfg.data_path.to_owned();
        let writer_thread = tokio::spawn(async move {
            let data_path = data_path;
            while let Some(data) = receiver.recv().await {
                if let Err(e) = data.write_parquet_folder(&data_path).await {
                    log::error!(
                        "failed to write parquet folder:\n{}\n quitting writer thread.",
                        e
                    );
                    break;
                }
            }
        });

        'ingest: while let Some(batches) = batches.next().await {
            let (block_ranges, block_batches, log_batches) = batches.map_err(Error::GetBatch)?;

            for ((block_range, block_batch), log_batch) in block_ranges
                .into_iter()
                .zip(block_batches.into_iter())
                .zip(log_batches.into_iter())
            {
                data.range += block_range;
                for mut block in block_batch.into_iter() {
                    for tx in mem::take(&mut block.transactions).into_iter() {
                        data.txs.push(tx);
                    }
                    data.blocks.push(block);
                }
                for log in log_batch.into_iter() {
                    data.logs.push(log);
                }

                #[allow(clippy::collapsible_if)]
                if data.blocks.len > self.cfg.max_blocks_per_file
                    || data.txs.len > self.cfg.max_txs_per_file
                    || data.logs.len > self.cfg.max_logs_per_file
                {
                    if sender.send(mem::take(&mut data)).await.is_err() {
                        log::info!("writer thread crashed. exiting ingest loop...");
                        break 'ingest;
                    }
                }
            }
        }

        if !writer_thread.is_finished() {
            log::info!("waiting for writer thread to finish...");
        }
        writer_thread.await.map_err(Error::RunWriterThread)?;

        todo!()
    }

    async fn delete_temp_dirs(dir_names: &[DirName]) -> Result<()> {
        log::info!("deleting temporary directories...");

        for name in dir_names.iter() {
            if name.is_temp {
                tokio::fs::remove_dir_all(&name.to_string())
                    .await
                    .map_err(Error::RemoveTempDir)?;
            }
        }

        Ok(())
    }

    fn get_start_block(dir_names: &[DirName]) -> Result<u32> {
        if dir_names.is_empty() {
            return Ok(0);
        }
        let first_range = dir_names[0].range;

        if first_range.from != 0 {
            return Err(Error::FolderRangeMismatch(0, 0));
        }

        let mut max = first_range.to;
        for (a, b) in dir_names.iter().tuple_windows() {
            max = cmp::max(max, b.range.to);

            if a.range.to != b.range.from {
                return Err(Error::FolderRangeMismatch(a.range.to, b.range.from));
            }
        }

        Ok(max)
    }
}

#[derive(Default)]
pub struct Data {
    blocks: Blocks,
    txs: Transactions,
    logs: Logs,
    range: BlockRange,
}

impl Data {
    async fn write_parquet_folder<P: AsRef<Path>>(self, path: P) -> Result<()> {
        todo!();
    }
}
