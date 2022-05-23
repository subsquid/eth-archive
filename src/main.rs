use eth_archive::eth_client::EthClient;
use eth_archive::eth_request::{GetBlockByNumber, GetLogs};
use eth_archive::parquet_writer::ParquetWriter;
use eth_archive::schema::{Blocks, Logs, Transactions};
use std::{fs, mem};
use std::sync::Arc;
use std::time::Instant;
use serde::Deserialize;
use eth_archive::retry::retry;
use std::path::{Path, PathBuf};

#[tokio::main]
async fn main() {
    let config = fs::read_to_string("EthArchive.toml").unwrap();
    let config: Config = toml::de::from_str(&config).unwrap();

    let db = rocksdb::DB::open_default(config.database_path()).unwrap();

    let client = EthClient::new(config.eth_rpc_url().clone()).unwrap();
    let client = Arc::new(client);

    let block_writer: ParquetWriter<Blocks> = ParquetWriter::new("data/block/block", 1_000_000);
    let tx_writer: ParquetWriter<Transactions> = ParquetWriter::new("data/tx/tx", 3_300_000);
    let log_writer: ParquetWriter<Logs> = ParquetWriter::new("data/log/log", 3_300_000);

    let block_range = 10_000_000..14_000_000;

    let block_tx_job = tokio::spawn({
        let client = client.clone();
        async move {
            const BATCH_SIZE: usize = 100;
            const STEP: usize = 100;

            for block_num in block_range.step_by(STEP * BATCH_SIZE) {
                let start = Instant::now();
                let group = (0..STEP)
                    .map(|step| {
                        let client = client.clone();
                        retry(move || {
                            let client = client.clone();
                            let start = block_num + step * BATCH_SIZE;
                            let end = start + BATCH_SIZE;

                            let batch = (start..end)
                                .map(|i| GetBlockByNumber { block_number: i })
                                .collect::<Vec<_>>();
                            async move { client.send_batch(&batch).await }
                        })
                    })
                    .collect::<Vec<_>>();

                let group = futures::future::join_all(group).await;

                for batch in group {
                    let mut batch = match batch {
                        Ok(batch) => batch,
                        Err(e) => {
                            eprintln!("failed batch block req: {:#?}", e);
                            continue;
                        }
                    };
                    for block in batch.iter_mut() {
                        tx_writer.send(mem::take(&mut block.transactions));
                    }
                    block_writer.send(batch);
                }
                println!(
                    "TX/BLOCK WRITER: processed {} blocks in {} ms",
                    STEP * BATCH_SIZE,
                    start.elapsed().as_millis()
                )
            }
        }
    });

    let block_range = 10_000_000..14_000_000;

    let log_job = tokio::spawn({
        let client = client.clone();
        async move {
            const BATCH_SIZE: usize = 5;
            const STEP: usize = 100;

            for block_num in block_range.step_by(STEP * BATCH_SIZE) {
                let start = Instant::now();
                let group = (0..STEP)
                    .map(|step| {
                        let start = block_num + step * BATCH_SIZE;
                        let end = start + BATCH_SIZE;

                        let client = client.clone();
                        async move {
                            client
                                .send(GetLogs {
                                    from_block: start,
                                    to_block: end,
                                })
                                .await
                        }
                    })
                    .collect::<Vec<_>>();

                let group = futures::future::join_all(group).await;

                for batch in group {
                    let batch = match batch {
                        Ok(batch) => batch,
                        Err(e) => {
                            eprintln!("failed batch log req: {:#?}", e);
                            continue;
                        }
                    };

                    log_writer.send(batch);
                }
                println!(
                    "LOG WRITER: processed {} blocks in {} ms",
                    STEP * BATCH_SIZE,
                    start.elapsed().as_millis()
                )
            }
        }
    });

    block_tx_job.await.unwrap();
    log_job.await.unwrap();
}

#[derive(Deserialize)]
pub struct Config {
    eth_rpc_url: url::Url,
    parquet_path: PathBuf,
    database_path: PathBuf,
}

impl Config {
    pub fn eth_rpc_url(&self) -> &reqwest::Url {
        &self.eth_rpc_url
    }

    pub fn parquet_path(&self) -> &Path {
        self.parquet_path.as_path()
    }

    pub fn database_path(&self) -> &Path {
        self.database_path.as_path()
    }
}
