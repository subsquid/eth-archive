use crate::config::Config;
use crate::schema::{
    block_schema, log_schema, parquet_write_options, tx_schema, Blocks, IntoChunks, Logs,
    Transactions,
};
use crate::server::Server;
use crate::{Error, Result};
use arrow2::datatypes::{DataType, Schema};
use arrow2::io::parquet::write::transverse;
use arrow2::io::parquet::write::Encoding;
use arrow2::io::parquet::write::FileSink;
use eth_archive_core::dir_name::DirName;
use eth_archive_core::eth_client::EthClient;
use eth_archive_core::ingest_metrics::IngestMetrics;
use eth_archive_core::rayon_async;
use eth_archive_core::retry::Retry;
use eth_archive_core::s3_sync;
use eth_archive_core::types::BlockRange;
use futures::channel::mpsc;
use futures::pin_mut;
use futures::stream::StreamExt;
use futures::SinkExt;
use itertools::Itertools;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::{cmp, mem};
use tokio::io::AsyncWriteExt;
use tokio::runtime::Runtime;
use futures::channel::mpsc;
use tokio_util::compat::TokioAsyncReadCompatExt;

pub struct Ingester {
    eth_client: Arc<EthClient>,
    cfg: Config,
    metrics: Arc<IngestMetrics>,
}

impl Ingester {
    pub async fn new(cfg: Config) -> Result<Self> {
        let retry = Retry::new(cfg.retry);
        let metrics = IngestMetrics::new();
        let metrics = Arc::new(metrics);
        let eth_client = EthClient::new(cfg.ingest.clone(), retry, metrics.clone())
            .map_err(Error::CreateEthClient)?;
        let eth_client = Arc::new(eth_client);

        let ingest_metrics = metrics.clone();
        std::thread::spawn(move || {
            let ingest_metrics = ingest_metrics.clone();
            let runtime = Runtime::new().unwrap();
            runtime.block_on(async move {
                loop {
                    if let Err(e) = Server::run(cfg.metrics_addr, ingest_metrics.clone()).await {
                        log::error!("failed to run server to serve metrics:\n{}", e);
                    }
                    tokio::time::sleep(Duration::from_secs(10)).await;
                }
            });
        });

        Ok(Self {
            eth_client,
            cfg,
            metrics,
        })
    }

    pub async fn run(&self) -> Result<()> {
        let data_path = &self.cfg.data_path;

        tokio::fs::create_dir_all(data_path)
            .await
            .map_err(Error::CreateMissingDirectories)?;

        let dir_names = DirName::delete_temp_and_list_sorted(data_path)
            .await
            .map_err(Error::ListFolderNames)?;

        let block_num = Self::get_start_block(&dir_names)?;

        log::info!("starting to ingest from {}", block_num);

        if let Some(s3_config) = self.cfg.s3.into_parsed() {
            s3_sync::start(s3_sync::Direction::Up, &self.cfg.data_path, &s3_config)
                .await
                .map_err(Error::StartS3Sync)?;
        } else {
            log::info!("no s3 config, disabling s3 sync");
        }

        let batches = self
            .eth_client
            .clone()
            .stream_batches(Some(block_num), None);
        pin_mut!(batches);

        let mut data = Data::default();

        let (mut sender, receiver): (mpsc::Sender<Data>, _) =
            mpsc::channel(self.cfg.max_pending_folder_writes);

        let config = self.cfg.clone();
        let ingest_metrics = self.metrics.clone();
        let write_concurrency = self.cfg.folder_write_concurrency;
        let writer_thread = tokio::spawn(async move {
            let config = config;
            let ingest_metrics = ingest_metrics;
            let stream = receiver.map(|data| {
                let config = config.clone();
                let ingest_metrics = ingest_metrics.clone();
                async move { data.write_parquet_folder(&config, &ingest_metrics).await }
            });

            let mut stream = stream.buffer_unordered(write_concurrency);

            while let Some(res) = stream.next().await {
                if let Err(e) = res {
                    log::error!(
                        "failed to write parquet folder. quitting writer thread:\n{}",
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
                data.range = match data.range {
                    Some(mut range) => {
                        range += block_range;
                        Some(range)
                    }
                    None => Some(block_range),
                };

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
                if data.blocks.len >= self.cfg.max_blocks_per_file
                    || data.txs.len >= self.cfg.max_txs_per_file
                    || data.logs.len >= self.cfg.max_logs_per_file
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
                return Ok(a.range.to);
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
    range: Option<BlockRange>,
}

impl Data {
    async fn write_parquet_folder(self, cfg: &Config, metrics: &IngestMetrics) -> Result<()> {
        let range = self.range.unwrap();

        let start_time = Instant::now();

        let mut temp_path = cfg.data_path.to_owned();
        temp_path.push(
            &DirName {
                range,
                is_temp: true,
            }
            .to_string(),
        );
        tokio::fs::create_dir(&temp_path)
            .await
            .map_err(Error::CreateDir)?;

        let block_fut = {
            let mut temp_path = temp_path.clone();
            temp_path.push("block.parquet");

            write_file(
                temp_path,
                Box::new(self.blocks),
                block_schema(),
                cfg.max_blocks_per_file / cfg.max_row_groups_per_file,
            )
        };

        let tx_fut = {
            let mut temp_path = temp_path.clone();
            temp_path.push("tx.parquet");

            write_file(
                temp_path,
                Box::new(self.txs),
                tx_schema(),
                cfg.max_txs_per_file / cfg.max_row_groups_per_file,
            )
        };

        let log_fut = {
            let mut temp_path = temp_path.clone();
            temp_path.push("log.parquet");

            write_file(
                temp_path,
                Box::new(self.logs),
                log_schema(),
                cfg.max_logs_per_file / cfg.max_row_groups_per_file,
            )
        };

        futures::future::try_join3(block_fut, tx_fut, log_fut).await?;

        let mut final_path = cfg.data_path.to_owned();
        final_path.push(
            &DirName {
                range,
                is_temp: false,
            }
            .to_string(),
        );

        tokio::fs::rename(&temp_path, &final_path)
            .await
            .map_err(Error::RenameDir)?;

        let elapsed = start_time.elapsed().as_millis();
        let blk_count = range.to - range.from;
        if elapsed > 0 && blk_count > 0 {
            metrics.record_write_speed(blk_count as f64 / elapsed as f64 * 1000.);
        }

        if range.to > 0 {
            metrics.record_write_height(range.to);
        }

        Ok(())
    }
}

fn encodings(schema: &Schema) -> Vec<Vec<Encoding>> {
    let encoding_map = |data_type: &DataType| match data_type {
        DataType::Binary | DataType::LargeBinary => Encoding::DeltaLengthByteArray,
        _ => Encoding::Plain,
    };

    schema
        .fields
        .iter()
        .map(|f| transverse(&f.data_type, encoding_map))
        .collect::<Vec<_>>()
}

async fn write_file<T: IntoChunks + Send + 'static>(
    temp_path: PathBuf,
    chunks: Box<T>,
    schema: Schema,
    items_per_chunk: usize,
) -> Result<()> {
    let chunks = rayon_async::spawn(move || chunks.into_chunks(items_per_chunk)).await;
    let mut file = tokio::fs::File::create(&temp_path)
        .await
        .map_err(Error::CreateFile)?
        .compat();
    let encodings = encodings(&schema);
    let mut writer = FileSink::try_new(&mut file, schema, encodings, parquet_write_options())
        .map_err(Error::CreateFileSink)?;
    let mut chunk_stream = futures::stream::iter(chunks);
    writer
        .send_all(&mut chunk_stream)
        .await
        .map_err(Error::WriteFileData)?;
    writer.close().await.map_err(Error::CloseFileSink)?;

    mem::drop(writer);

    let mut file = file.into_inner();

    file.flush().await.map_err(Error::CreateFile)?;
    file.sync_all().await.map_err(Error::CreateFile)?;

    Ok(())
}
