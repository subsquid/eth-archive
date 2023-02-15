use crate::config::ParsedS3Config;
use crate::dir_name::DirName;
use crate::retry::Retry;
use crate::{Error, Result};
use aws_config::retry::RetryConfig;
use futures::Future;
use futures::{StreamExt, TryStreamExt};
use std::collections::BTreeSet;
use std::io;
use std::path::Path;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

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

    pub async fn spawn_sync(self: Arc<Self>, direction: Direction, data_path: &Path) -> Result<()> {
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

        Ok(())
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

        for s3_name in self.clone().get_list().await?.iter() {
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
        let s3_names = self.clone().get_list().await?;

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

    pub async fn get_bytes(
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

    async fn get_file(self: Arc<Self>, path: Arc<Path>, s3_path: Arc<str>) -> Result<File> {
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

    pub async fn get_list(self: Arc<Self>) -> Result<BTreeSet<String>> {
        self.retry
            .retry(|| {
                let s3_client = self.clone();
                async move { s3_client.get_list_impl().await }
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

    async fn get_file_impl(&self, path: &Path, s3_path: &str) -> Result<File> {
        let file = File::create(&path).await.map_err(Error::OpenFile)?;

        self.client
            .get_object()
            .bucket(&self.config.s3_bucket_name)
            .key(s3_path)
            .send()
            .await
            .map_err(Error::S3Get)?
            .body
            .map_err(|_| Error::S3GetObjChunk)
            .try_fold(file, |mut file, chunk| async move {
                file.write_all(&chunk).await.map_err(Error::WriteFile)?;
                Ok(file)
            })
            .await
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

    async fn get_list_impl(&self) -> Result<BTreeSet<String>> {
        let mut s3_names = BTreeSet::new();
        let mut stream = self
            .client
            .list_objects_v2()
            .bucket(&self.config.s3_bucket_name)
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

pub fn parse_s3_name(s3_name: &str) -> (DirName, String) {
    let mut s3_name_parts = s3_name.split('/');
    let dir_name = s3_name_parts.next().unwrap();
    let dir_name = DirName::from_str(dir_name).unwrap();
    let file_name = s3_name_parts.next().unwrap().to_owned();

    (dir_name, file_name)
}
