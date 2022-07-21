use crate::config::{Config, IngestConfig};
use crate::db::DbHandle;
use crate::options::Options;
use crate::parquet_writer::ParquetWriter;
use crate::schema::{Blocks, Logs, Transactions};
use crate::{Error, Result};
use eth_archive_core::eth_client::EthClient;
use eth_archive_core::eth_request::GetBlockByNumber;
use eth_archive_core::retry::Retry;
use std::sync::Arc;
use std::time::Instant;

pub struct ParquetWriterRunner {
    db: Arc<DbHandle>,
    cfg: IngestConfig,
    eth_client: Arc<EthClient>,
    block_writer: ParquetWriter<Blocks>,
    transaction_writer: ParquetWriter<Transactions>,
    log_writer: ParquetWriter<Logs>,
    retry: Retry,
}

impl ParquetWriterRunner {
    pub async fn new(options: &Options) -> Result<Self> {
        let config = tokio::fs::read_to_string(&options.cfg_path)
            .await
            .map_err(Error::ReadConfigFile)?;

        let config: Config = toml::de::from_str(&config).map_err(Error::ParseConfig)?;

        let db = DbHandle::new(&config.db)
            .await
            .map_err(|e| Error::CreateDbHandle(Box::new(e)))?;
        let db = Arc::new(db);

        let eth_client =
            EthClient::new(config.ingest.eth_rpc_url.clone()).map_err(Error::CreateEthClient)?;
        let eth_client = Arc::new(eth_client);

        let block_writer = ParquetWriter::new(config.block);
        let transaction_writer = ParquetWriter::new(config.transaction);
        let log_writer = ParquetWriter::new(config.log);

        let retry = Retry::new(config.retry);

        Ok(Self {
            db,
            cfg: config.ingest,
            eth_client,
            block_writer,
            transaction_writer,
            log_writer,
            retry,
        })
    }

    pub async fn run(&self) -> Result<()> {
        let from_block = self.initial_sync().await?;
        todo!();
    }

    pub async fn initial_sync(&self) -> Result<usize> {
        let from_block = 0;
        let to_block = self
            .db
            .get_min_block_number()
            .await?
            .ok_or(Error::NoBlocksInDb)?;
        log::info!(
            "starting initial sync. from: {}, to: {}.",
            from_block,
            to_block
        );

        let step = self.cfg.http_req_concurrency * self.cfg.block_batch_size;
        for block_num in (from_block..to_block).step_by(step) {
            let concurrency = self.cfg.http_req_concurrency;
            let batch_size = self.cfg.block_batch_size;
            let start_time = Instant::now();
            let group = (0..concurrency)
                .map(|step| {
                    let eth_client = self.eth_client.clone();
                    let start = block_num + step * batch_size;
                    let end = start + batch_size;
                    async move {
                        self.retry
                            .retry(move || {
                                let eth_client = eth_client.clone();
                                async move {
                                    let batch = (start..end)
                                        .map(|i| GetBlockByNumber { block_number: i })
                                        .collect::<Vec<_>>();
                                    eth_client
                                        .send_batch(&batch)
                                        .await
                                        .map_err(Error::EthClient)
                                }
                            })
                            .await
                    }
                })
                .collect::<Vec<_>>();
            let group = futures::future::join_all(group).await;
            log::info!(
                "downloaded {} blocks in {}ms",
                step,
                start_time.elapsed().as_millis()
            );
            let start_time = Instant::now();
            for batch in group {
                let batch = match batch {
                    Ok(batch) => batch,
                    Err(e) => {
                        log::error!("failed batch block req: {:#?}", e);
                        continue;
                    }
                };

                todo!()
            }
            log::info!(
                "inserted {} blocks in {}ms",
                step,
                start_time.elapsed().as_millis()
            );
        }
        log::info!("finished initial sync up to block {}", to_block);
        Ok(to_block)
    }
}
