mod util;
mod ver0_0_39;

pub fn get_parquet_source(ver: FormatVersion) -> Box<dyn ParquetSource> {
    match ver {
        FormatVersion::Ver0_0_39 => Box::new(ver0_0_39::Ver0_0_39),
    }
}

trait ParquetSource {
    fn read_blocks(
        columns: impl Iterator<Item = Result<Vec<ArrayIter<'_>>>>,
    ) -> Result<BTreeMap<u32, Block>>;

    fn read_txs(
        columns: impl Iterator<Item = Result<Vec<ArrayIter<'_>>>>,
    ) -> Result<Vec<Transaction>>;

    fn read_logs(columns: impl Iterator<Item = Result<Vec<ArrayIter<'_>>>>) -> Result<Vec<Log>>;

    fn block_fields() -> Vec<Field>;

    fn tx_fields() -> Vec<Field>;

    fn log_fields() -> Vec<Field>;
}

fn block_not_found_err(block_num: u32) -> Result<()> {
    Err(Error::BlockNotFoundInS3(block_num))
}

fn stream_batches(
    retry: Retry,
    ingest_metrics: Arc<IngestMetrics>,
    start_block: u32,
    s3_src_bucket: Arc<str>,
    client: Arc<aws_sdk_s3::Client>,
    dir_names: Vec<DirName>,
) -> impl Stream<Item = Result<(Vec<BlockRange>, Vec<Vec<Block>>, Vec<Vec<Log>>)>> {
    let num_files = dir_names.len();

    log::info!("s3_sync_ingest: {} directories to sync.", num_files);

    let mut block_num = start_block;
    let mut start_time = Instant::now();

    async_stream::try_stream! {
        for (i, dir_name) in dir_names.into_iter().enumerate() {
            // s3 files have a gap in them
            if dir_name.range.from > block_num {
                // This is a function a call to make the macro work
                block_not_found_err(block_num)?;
            }

            let start = Instant::now();

            let block_fut = retry
                .retry(|| {
                    let s3_src_bucket = s3_src_bucket.clone();
                    let client = client.clone();
                    async move {
                        read_blocks(
                            dir_name,
                            s3_src_bucket,
                            client,
                        ).await
                    }
                });

            let tx_fut = retry
                .retry(|| {
                    let s3_src_bucket = s3_src_bucket.clone();
                    let client = client.clone();
                    async move {
                        read_txs(
                            dir_name,
                            s3_src_bucket,
                            client,
                        ).await
                    }
                });

            let log_fut = retry
                .retry(|| {
                    let s3_src_bucket = s3_src_bucket.clone();
                    let client = client.clone();
                    async move {
                        read_logs(
                            dir_name,
                            s3_src_bucket,
                            client,
                        ).await
                    }
                });

            let (mut blocks, txs, logs) = futures::future::try_join3(block_fut, tx_fut, log_fut).await.map_err(Error::Retry)?;

            let block_range = BlockRange {
                from: *blocks.first_key_value().unwrap().0,
                to: blocks.last_key_value().unwrap().0 + 1,
            };

            for tx in txs {
                blocks.get_mut(&tx.block_number.0).unwrap().transactions.push(tx);
            }

            let blocks = blocks.into_values().collect::<Vec<_>>();

            block_num = dir_name.range.to;

            if block_num > 0 {
                ingest_metrics.record_download_height(block_num-1);
            }
            let elapsed = start.elapsed().as_millis();
            if elapsed > 0 {
                ingest_metrics.record_download_speed((block_num-dir_name.range.from) as f64 / elapsed as f64 * 1000.);
            }

            if start_time.elapsed().as_secs() > 15 {
                let percentage = (i + 1) as f64 / num_files as f64 * 100.;
                log::info!(
                    "s3_sync_ingest progress: {}/{} {:.2}%",
                    i + 1,
                    num_files,
                    percentage
                );
                start_time = Instant::now();
            }

            yield (vec![block_range], vec![blocks], vec![logs]);
        }
    }
}

pub async fn execute(
    retry: Retry,
    ingest_metrics: Arc<IngestMetrics>,
    start_block: u32,
    s3_src_bucket: Arc<str>,
    client: Arc<aws_sdk_s3::Client>,
) -> Result<Data> {
    let list = get_list(&s3_src_bucket, &client)
        .await
        .map_err(Error::ListS3BucketContents)?;

    let mut dir_names: BTreeMap<u32, (u8, DirName)> = BTreeMap::new();

    for s3_name in list.iter() {
        let (dir_name, _) = parse_s3_name(s3_name);
        dir_names
            .entry(dir_name.range.from)
            .or_insert((0, dir_name))
            .0 += 1;
    }

    let dir_names = dir_names
        .into_iter()
        // Check that this dir has all parquet files in s3 and is relevant considering our start_block
        .filter(|(_, (val, dir_name))| *val == 3 && dir_name.range.to > start_block)
        .map(|(_, (_, dir_name))| dir_name)
        .collect::<Vec<_>>();

    let data = stream_batches(
        retry,
        ingest_metrics,
        start_block,
        s3_src_bucket,
        client,
        dir_names,
    );

    Ok(Box::pin(data))
}
