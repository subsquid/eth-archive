use eth_archive::config::Config;
use eth_archive::eth_client::EthClient;
use eth_archive::eth_request::{GetBlockByNumber, GetLogs};
use eth_archive::parquet_writer::ParquetWriter;
use eth_archive::retry::retry;
use eth_archive::schema::{Blocks, Logs, Transactions};
use std::sync::Arc;
use std::time::Instant;
use std::{cmp, fs, mem};

const NEXT_BLOCK_NUM_MARKER: &str = "0d535da7-73c7-4991-b16f-80d2904f569e";
const BLOCK: &str = "block";
const TX: &str = "tx";
const LOG: &str = "log";

#[tokio::main]
async fn main() {
    let config = fs::read_to_string("EthArchive.toml").unwrap();
    let config: Config = toml::de::from_str(&config).unwrap();

    let mut database_path = config.data_path.clone();
    database_path.push(&config.database_path);

    let mut db_options = rocksdb::Options::default();
    db_options.create_if_missing(true);
    let db = rocksdb::DB::open_cf(&db_options, &database_path, [BLOCK, LOG]).unwrap();
    let db = Arc::new(db);

    let client = EthClient::new(config.eth_rpc_url).unwrap();
    let client = Arc::new(client);

    let block_writer: ParquetWriter<Blocks> =
        ParquetWriter::new(BLOCK, &config.data_path, config.block.block_write_threshold);
    let tx_writer: ParquetWriter<Transactions> =
        ParquetWriter::new(TX, &config.data_path, config.block.tx_write_threshold);
    let log_writer: ParquetWriter<Logs> =
        ParquetWriter::new(LOG, &config.data_path, config.log.log_write_threshold);

    let cf_handle = db.cf_handle(BLOCK).unwrap();
    let next_block_num = match db
        .get_cf(cf_handle, NEXT_BLOCK_NUM_MARKER.as_bytes())
        .unwrap()
    {
        Some(bytes) => usize::from_le_bytes(bytes.try_into().unwrap()),
        None => 0,
    };
    let next_block_num = cmp::max(next_block_num, config.start_block);
    let block_range = next_block_num..config.end_block;

    let block_tx_job = tokio::spawn({
        let db = db.clone();
        let client = client.clone();
        async move {
            let batch_size = config.block.batch_size;
            let concurrency = config.block.concurrency;

            for block_num in block_range.step_by(concurrency * batch_size) {
                db.put_cf(
                    db.cf_handle(BLOCK).unwrap(),
                    NEXT_BLOCK_NUM_MARKER.as_bytes(),
                    block_num.to_le_bytes(),
                )
                .unwrap();

                let start = Instant::now();
                let group = (0..concurrency)
                    .map(|step| {
                        let db = db.clone();
                        let client = client.clone();
                        retry(
                            config.retry.num_tries,
                            config.retry.secs_between_tries,
                            move || {
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
                                            let cf_handle = db.cf_handle(BLOCK).unwrap();
                                            let mut key = start.to_le_bytes().to_vec();
                                            key.extend_from_slice(end.to_le_bytes().as_slice());
                                            db.put_cf(cf_handle, key, &[]).unwrap();
                                            Err(e)
                                        }
                                    }
                                }
                            },
                        )
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

    let cf_handle = db.cf_handle(LOG).unwrap();
    let next_block_num = match db.get_cf(cf_handle, NEXT_BLOCK_NUM_MARKER.as_bytes()) {
        Ok(Some(bytes)) => usize::from_le_bytes(bytes.try_into().unwrap()),
        _ => 0,
    };
    let next_block_num = cmp::max(next_block_num, config.start_block);
    let block_range = next_block_num..config.end_block;

    let log_job = tokio::spawn({
        let db = db.clone();
        let client = client.clone();
        async move {
            let batch_size = config.log.batch_size;
            let concurrency = config.log.concurrency;

            for block_num in block_range.step_by(concurrency * batch_size) {
                db.put_cf(
                    db.cf_handle(LOG).unwrap(),
                    NEXT_BLOCK_NUM_MARKER.as_bytes(),
                    block_num.to_le_bytes(),
                )
                .unwrap();

                let start = Instant::now();
                let group = (0..concurrency)
                    .map(|step| {
                        let db = db.clone();
                        let client = client.clone();
                        retry(
                            config.retry.num_tries,
                            config.retry.secs_between_tries,
                            move || {
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
                                            let cf_handle = db.cf_handle(LOG).unwrap();
                                            let mut key = start.to_le_bytes().to_vec();
                                            key.extend_from_slice(end.to_le_bytes().as_slice());
                                            db.put_cf(cf_handle, key, &[]).unwrap();
                                            Err(e)
                                        }
                                    }
                                }
                            },
                        )
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
