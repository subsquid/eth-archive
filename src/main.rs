use eth_archive::config::Config;
use eth_archive::eth_client::EthClient;
use eth_archive::eth_request::{GetBlockByNumber, GetLogs};
use eth_archive::parquet_writer::ParquetWriter;
use eth_archive::retry::retry;
use eth_archive::schema::{Blocks, Logs, Transactions};
use std::sync::Arc;
use std::time::Instant;
use std::{fs, mem};

#[tokio::main]
async fn main() {
    let config = fs::read_to_string("EthArchive.toml").unwrap();
    let config: Config = toml::de::from_str(&config).unwrap();

    let db = rocksdb::DB::open_default(&config.database_path).unwrap();
    let db = Arc::new(db);

    let client = EthClient::new(config.eth_rpc_url).unwrap();
    let client = Arc::new(client);

    let block_writer: ParquetWriter<Blocks> = ParquetWriter::new(
        "block",
        &config.parquet_path,
        config.block.block_write_threshold,
    );
    let tx_writer: ParquetWriter<Transactions> =
        ParquetWriter::new("tx", &config.parquet_path, config.block.tx_write_threshold);
    let log_writer: ParquetWriter<Logs> =
        ParquetWriter::new("log", &config.parquet_path, config.log.log_write_threshold);

    let block_range = config.start_block..config.end_block;

    let block_tx_job = tokio::spawn({
        let db = db.clone();
        let client = client.clone();
        async move {
            let batch_size = config.block.batch_size;
            let concurrency = config.block.concurrency;

            for block_num in block_range.step_by(concurrency * batch_size) {
                let start = Instant::now();
                let group = (0..concurrency)
                    .map(|step| {
                        let db = db.clone();
                        let client = client.clone();
                        retry(move || {
                            let db = db.clone();
                            let client = client.clone();
                            let start = block_num + step * batch_size;
                            let end = start + batch_size;

                            let batch = (start..end)
                                .map(|i| GetBlockByNumber { block_number: i })
                                .collect::<Vec<_>>();
                            async move {
                                match client.send_batch(&batch).await {
                                    Ok(res) => Ok(res),
                                    Err(e) => {
                                        let cf_handle = db.cf_handle("block").unwrap();
                                        let mut key = start.to_le_bytes().to_vec();
                                        key.extend_from_slice(end.to_le_bytes().as_slice());
                                        db.put_cf(cf_handle, key, &[]).unwrap();
                                        Err(e)
                                    }
                                }
                            }
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
                    concurrency * batch_size,
                    start.elapsed().as_millis()
                )
            }
        }
    });

    let block_range = config.start_block..config.end_block;

    let log_job = tokio::spawn({
        let db = db.clone();
        let client = client.clone();
        async move {
            let batch_size = config.log.batch_size;
            let concurrency = config.log.concurrency;

            for block_num in block_range.step_by(concurrency * batch_size) {
                let start = Instant::now();
                let group = (0..concurrency)
                    .map(|step| {
                        let db = db.clone();
                        let client = client.clone();
                        retry(move || {
                            let db = db.clone();
                            let client = client.clone();

                            let start = block_num + step * batch_size;
                            let end = start + batch_size;

                            async move {
                                let res = client
                                    .send(GetLogs {
                                        from_block: start,
                                        to_block: end,
                                    })
                                    .await;
                                match res {
                                    Ok(res) => Ok(res),
                                    Err(e) => {
                                        let cf_handle = db.cf_handle("log").unwrap();
                                        let mut key = start.to_le_bytes().to_vec();
                                        key.extend_from_slice(end.to_le_bytes().as_slice());
                                        db.put_cf(cf_handle, key, &[]).unwrap();
                                        Err(e)
                                    }
                                }
                            }
                        })
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
                    concurrency * batch_size,
                    start.elapsed().as_millis()
                )
            }
        }
    });

    block_tx_job.await.unwrap();
    log_job.await.unwrap();
}
