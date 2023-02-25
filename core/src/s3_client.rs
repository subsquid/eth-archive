use crate::config::ParsedS3Config;
use crate::dir_name::DirName;
use crate::ingest_metrics::IngestMetrics;
use crate::parquet_source::{self, read_parquet_buf, ParquetSource};
use crate::retry::Retry;
use crate::types::{Block, BlockRange, FormatVersion, Log};
use crate::{Error, Result};
use aws_config::retry::RetryConfig;
use futures::{Future, Stream, TryFutureExt};
use futures::{StreamExt, TryStreamExt};
use polars::export::arrow::datatypes::Field;
use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::path::Path;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub struct S3Client {
    retry: Retry,
    config: ParsedS3Config,
    client: aws_sdk_s3::Client,
}

impl S3Client {
    pub async fn new(retry: Retry, config: &ParsedS3Config) -> Result<Self> {
        let cfg = aws_config::from_env()
            .retry_config(RetryConfig::standard())
            .endpoint_url(&config.s3_endpoint)
            .region(aws_types::region::Region::new(
                config.s3_bucket_region.clone(),
            ))
            .load()
            .await;
        let client = aws_sdk_s3::Client::new(&cfg);

        Ok(Self {
            retry,
            config: config.clone(),
            client,
        })
    }

    pub fn spawn_s3_sync(self: Arc<Self>, direction: Direction, data_path: &Path) {
        let data_path = data_path.to_owned();

        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(Duration::from_secs(self.config.s3_sync_interval_secs));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

            loop {
                interval.tick().await;

                log::info!("starting sync...");

                let start_time = Instant::now();

                let res = self.clone().execute(direction, &data_path).await;

                let elapsed = start_time.elapsed().as_secs();

                if let Err(e) = res {
                    log::error!("failed to execute sync in {} seconds:\n{}", elapsed, e);
                } else {
                    log::info!("finished sync in {} seconds.", elapsed);
                }
            }
        });
    }

    pub async fn stream_batches(
        self: Arc<Self>,
        ingest_metrics: Arc<IngestMetrics>,
        start_block: u32,
        s3_src_bucket: &str,
        format_version: &str,
    ) -> Result<BatchStream> {
        let source = parquet_source::get(FormatVersion::from_str(format_version)?);
        let dir_names = Self::get_dir_names_from_list(
            start_block,
            &self.clone().get_list(s3_src_bucket.into()).await?,
        );

        let batch_stream = self.stream_batches_impl(
            ingest_metrics,
            start_block,
            s3_src_bucket,
            source,
            dir_names,
        );

        Ok(Box::pin(batch_stream))
    }

    fn stream_batches_impl(
        self: Arc<Self>,
        ingest_metrics: Arc<IngestMetrics>,
        start_block: u32,
        s3_src_bucket: &str,
        source: Box<dyn ParquetSource>,
        dir_names: Vec<DirName>,
    ) -> impl Stream<Item = Result<(Vec<BlockRange>, Vec<Vec<Block>>, Vec<Vec<Log>>)>> {
        let num_files = dir_names.len();

        log::info!("stream_batches: {} directories to stream.", num_files);

        let mut block_num = start_block;
        let mut start_time = Instant::now();

        let s3_src_bucket: Arc<str> = s3_src_bucket.into();

        async_stream::try_stream! {
            for (i, dir_name) in dir_names.into_iter().enumerate() {
                // s3 files have a gap in them
                if dir_name.range.from > block_num {
                    fn block_not_found_err(block_num: u32) -> Result<()> {
                        Err(Error::BlockNotFoundInS3(block_num))
                    }
                    // This is a function call to make the macro work
                    block_not_found_err(block_num)?;
                }

                let start = Instant::now();

                let read_fut = |kind, fields: Vec<Field>| {
                    let s3_key = format!("{dir_name}/{kind}.parquet");
                    let s3_client = self.clone();
                    let s3_src_bucket = s3_src_bucket.clone();

                    async move {
                        let file = s3_client.clone().get_bytes(s3_key.into(), s3_src_bucket).await?;
                        read_parquet_buf(&file, fields)
                    }
                };

                let block_fut = read_fut("block", source.block_fields()).map_ok(|columns| source.read_blocks(columns));
                let tx_fut = read_fut("tx", source.tx_fields()).map_ok(|columns| source.read_txs(columns));
                let log_fut = read_fut("log", source.log_fields()).map_ok(|columns| source.read_logs(columns));

                let (mut blocks, txs, logs) = futures::future::try_join3(block_fut, tx_fut, log_fut).await?;

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
                        "stream_batches progress: {}/{} {:.2}%",
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

    async fn execute(self: Arc<Self>, direction: Direction, data_path: &Path) -> Result<()> {
        // delete temp directories if we are syncing down, since we own them
        if let Direction::Down = direction {
            DirName::delete_temp(data_path).await?;
        }

        let concurrency = self.config.s3_concurrency.get();

        let futs = match direction {
            Direction::Up => self.sync_files_to_s3(data_path).await?,
            Direction::Down => self.sync_files_from_s3(data_path).await?,
        };

        let num_files = futs.len();

        log::info!("{} files to sync.", num_files);

        let stream = futures::stream::iter(futs).buffer_unordered(concurrency);
        futures::pin_mut!(stream);

        let mut start_time = Instant::now();
        let mut i = 0;

        while let Some(res) = stream.next().await {
            res?;

            if start_time.elapsed().as_secs() > 15 {
                let percentage = (i + 1) as f64 / num_files as f64 * 100.;
                log::info!(
                    "s3 sync progress: {}/{} {:.2}%",
                    i + 1,
                    num_files,
                    percentage
                );
                start_time = Instant::now();
            }

            i += 1;
        }

        // delete temp directories if we are syncing down, since we own them
        if let Direction::Down = direction {
            DirName::delete_temp(data_path).await?;
        }

        Ok(())
    }

    async fn sync_files_from_s3(self: Arc<Self>, data_path: &Path) -> Result<FileFutures> {
        let mut futs: FileFutures = Vec::new();

        for s3_name in self
            .clone()
            .get_list(self.config.s3_bucket_name.as_str().into())
            .await?
            .iter()
        {
            let (dir_name, file_name) = parse_s3_name(s3_name);

            let mut path = data_path.to_owned();
            path.push(dir_name.to_string());
            path.push(&file_name);

            match tokio::fs::File::open(&path).await {
                Err(e) if e.kind() == io::ErrorKind::NotFound => (),
                Ok(_) => continue,
                Err(e) => return Err(Error::OpenFile(e))?,
            }

            let s3_client = self.clone();
            let data_path = data_path.to_owned();

            futs.push(Box::pin(async move {
                let temp_path = {
                    let mut path = data_path.clone();
                    path.push(
                        DirName {
                            is_temp: true,
                            ..dir_name
                        }
                        .to_string(),
                    );

                    tokio::fs::create_dir_all(&path)
                        .await
                        .map_err(Error::CreateMissingDirectories)?;

                    path.push(&file_name);

                    path
                };

                s3_client
                    .get_file(
                        temp_path.clone().into(),
                        format!("{}/{}", dir_name, &file_name).into(),
                    )
                    .await?;

                let final_path = {
                    let mut path = data_path;
                    path.push(dir_name.to_string());

                    tokio::fs::create_dir_all(&path)
                        .await
                        .map_err(Error::CreateMissingDirectories)?;

                    path.push(&file_name);

                    path
                };

                tokio::fs::rename(&temp_path, &final_path)
                    .await
                    .map_err(Error::RenameFile)?;

                Ok(())
            }));
        }

        Ok(futs)
    }

    async fn sync_files_to_s3(self: Arc<Self>, data_path: &Path) -> Result<FileFutures> {
        let dir_names = DirName::list_sorted(data_path).await?;
        let s3_names = self
            .clone()
            .get_list(self.config.s3_bucket_name.as_str().into())
            .await?;

        let mut futs: FileFutures = Vec::new();

        for dir_name in dir_names {
            if dir_name.is_temp {
                continue;
            }

            for kind in ["block", "tx", "log"] {
                let s3_path = format!("{dir_name}/{kind}.parquet");
                if s3_names.contains(s3_path.as_str()) {
                    continue;
                }

                let mut path = data_path.to_owned();
                path.push(dir_name.to_string());
                path.push(format!("{kind}.parquet"));

                let s3_client = self.clone();

                let fut = async move { s3_client.put_file(path.into(), s3_path.into()).await };

                futs.push(Box::pin(fut));
            }
        }

        Ok(futs)
    }

    fn get_dir_names_from_list(start_block: u32, list: &BTreeSet<String>) -> Vec<DirName> {
        let mut dir_names: BTreeMap<u32, (u8, DirName)> = BTreeMap::new();

        for s3_name in list.iter() {
            let (dir_name, _) = parse_s3_name(s3_name);
            dir_names
                .entry(dir_name.range.from)
                .or_insert((0, dir_name))
                .0 += 1;
        }

        dir_names
            .into_iter()
            // Check that this dir has all parquet files in s3 and is relevant considering our start_block
            .filter(|(_, (val, dir_name))| *val == 3 && dir_name.range.to > start_block)
            .map(|(_, (_, dir_name))| dir_name)
            .collect::<Vec<_>>()
    }

    async fn get_bytes(
        self: Arc<Self>,
        s3_path: Arc<str>,
        s3_src_bucket: Arc<str>,
    ) -> Result<Vec<u8>> {
        self.retry
            .retry(|| {
                let s3_client = self.clone();
                let s3_path = s3_path.clone();
                let s3_src_bucket = s3_src_bucket.clone();

                async move { s3_client.get_bytes_impl(&s3_path, &s3_src_bucket).await }
            })
            .await
            .map_err(Error::Retry)
    }

    async fn get_file(self: Arc<Self>, path: Arc<Path>, s3_path: Arc<str>) -> Result<()> {
        self.retry
            .retry(|| {
                let s3_client = self.clone();
                let path = path.clone();
                let s3_path = s3_path.clone();

                async move { s3_client.get_file_impl(&path, &s3_path).await }
            })
            .await
            .map_err(Error::Retry)
    }

    async fn put_file(self: Arc<Self>, path: Arc<Path>, s3_path: Arc<str>) -> Result<()> {
        self.retry
            .retry(|| {
                let s3_client = self.clone();
                let path = path.clone();
                let s3_path = s3_path.clone();

                async move { s3_client.put_file_impl(&path, &s3_path).await }
            })
            .await
            .map_err(Error::Retry)
    }

    async fn get_list(self: Arc<Self>, bucket_name: Arc<str>) -> Result<BTreeSet<String>> {
        self.retry
            .retry(|| {
                let s3_client = self.clone();
                let bucket_name = bucket_name.clone();
                async move { s3_client.get_list_impl(&bucket_name).await }
            })
            .await
            .map_err(Error::Retry)
    }

    async fn get_bytes_impl(&self, s3_path: &str, s3_src_bucket: &str) -> Result<Vec<u8>> {
        let data = self
            .client
            .get_object()
            .bucket(s3_src_bucket)
            .key(s3_path)
            .send()
            .await
            .map_err(Error::S3Get)?
            .body
            .map_err(|_| Error::S3GetObjChunk)
            .try_fold(Vec::new(), |mut data, chunk| async move {
                data.extend_from_slice(&chunk);
                Ok(data)
            })
            .await?;

        Ok(data)
    }

    async fn get_file_impl(&self, path: &Path, s3_path: &str) -> Result<()> {
        let bytes = self
            .client
            .get_object()
            .bucket(&self.config.s3_bucket_name)
            .key(s3_path)
            .send()
            .await
            .map_err(Error::S3Get)?
            .body
            .map_err(|_| Error::S3GetObjChunk)
            .try_fold(Vec::new(), |mut bytes, chunk| async move {
                bytes.extend_from_slice(&chunk);
                Ok(bytes)
            })
            .await?;

        tokio::fs::write(path, &bytes)
            .await
            .map_err(Error::WriteFile)?;

        Ok(())
    }

    async fn put_file_impl(&self, path: &Path, s3_path: &str) -> Result<()> {
        let file = aws_sdk_s3::types::ByteStream::read_from()
            .path(path)
            .build()
            .await
            .unwrap();

        self.client
            .put_object()
            .bucket(&self.config.s3_bucket_name)
            .key(s3_path)
            .body(file)
            .send()
            .await
            .map_err(Error::S3Put)?;

        Ok(())
    }

    async fn get_list_impl(&self, bucket_name: &str) -> Result<BTreeSet<String>> {
        let mut s3_names = BTreeSet::new();
        let mut stream = self
            .client
            .list_objects_v2()
            .bucket(bucket_name)
            .into_paginator()
            .send();
        while let Some(res) = stream.next().await {
            let res = res.map_err(Error::S3List)?;
            if let Some(objs) = res.contents() {
                for obj in objs {
                    if let Some(key) = obj.key() {
                        s3_names.insert(key.to_owned());
                    }
                }
            }
        }

        Ok(s3_names)
    }
}

#[derive(Clone, Copy)]
pub enum Direction {
    Up,
    Down,
}

type FileFutures = Vec<Pin<Box<dyn Future<Output = Result<()>> + Send>>>;

fn parse_s3_name(s3_name: &str) -> (DirName, String) {
    let mut s3_name_parts = s3_name.split('/');
    let dir_name = s3_name_parts.next().unwrap();
    let dir_name = DirName::from_str(dir_name).unwrap();
    let file_name = s3_name_parts.next().unwrap().to_owned();

    (dir_name, file_name)
}

pub type BatchStream =
    Pin<Box<dyn Stream<Item = Result<(Vec<BlockRange>, Vec<Vec<Block>>, Vec<Vec<Log>>)>>>>;
