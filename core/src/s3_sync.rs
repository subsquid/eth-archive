use crate::config::ParsedS3Config;
use crate::dir_name::DirName;
use crate::{Error, Result};
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
use tokio::io::AsyncWriteExt;

#[derive(Clone, Copy)]
pub enum Direction {
    Up,
    Down,
}

pub async fn start(direction: Direction, data_path: &Path, config: &ParsedS3Config) -> Result<()> {
    let cfg = aws_config::from_env()
        .endpoint_url(&config.s3_endpoint)
        .region(aws_types::region::Region::new(
            config.s3_bucket_region.clone(),
        ))
        .load()
        .await;
    let client = aws_sdk_s3::Client::new(&cfg);
    let client = Arc::new(client);

    let bucket = config.s3_bucket_name.clone();
    let data_path = data_path.to_owned();

    let sync_interval = config.s3_sync_interval_secs;

    let s3_concurrency = config.s3_concurrency.get();

    tokio::spawn(async move {
        let mut start_time = Instant::now();

        loop {
            log::info!("starting sync...");

            let res = execute(
                direction,
                &data_path,
                bucket.clone().into(),
                client.clone(),
                s3_concurrency,
            )
            .await;

            let elapsed = start_time.elapsed().as_secs();

            if let Err(e) = res {
                log::error!("failed to execute sync:\n{}", e);
            } else {
                log::info!("finished sync in {} seconds.", elapsed);
            }

            if elapsed < sync_interval {
                tokio::time::sleep(Duration::from_secs(sync_interval - elapsed)).await;
            }
            start_time = Instant::now();
        }
    });

    Ok(())
}

async fn execute(
    direction: Direction,
    data_path: &Path,
    bucket: Arc<str>,
    client: Arc<aws_sdk_s3::Client>,
    concurrency: usize,
) -> Result<()> {
    // delete temp directories if we are syncing down, since we own them
    if let Direction::Down = direction {
        DirName::delete_temp(data_path).await?;
    }

    let futs = match direction {
        Direction::Up => sync_files_to_s3(data_path, bucket, client).await?,
        Direction::Down => sync_files_from_s3(data_path, bucket, client).await?,
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

type FileFutures = Vec<Pin<Box<dyn Future<Output = Result<()>> + Send>>>;

async fn sync_files_from_s3(
    data_path: &Path,
    bucket: Arc<str>,
    client: Arc<aws_sdk_s3::Client>,
) -> Result<FileFutures> {
    let mut futs: FileFutures = Vec::new();

    for s3_name in get_list(&bucket, &client).await?.iter() {
        let mut s3_name_parts = s3_name.split('/');
        let dir_name = s3_name_parts.next().unwrap();
        let dir_name = DirName::from_str(dir_name).unwrap();
        let file_name = s3_name_parts.next().unwrap().to_owned();

        let mut path = data_path.to_owned();
        path.push(dir_name.to_string());
        path.push(&file_name);

        match tokio::fs::File::open(&path).await {
            Err(e) if e.kind() == io::ErrorKind::NotFound => (),
            Ok(_) => continue,
            Err(e) => return Err(Error::OpenFile(e))?,
        }

        let client = client.clone();
        let data_path = data_path.to_owned();
        let bucket = bucket.clone();

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

            let file = tokio::fs::File::create(&temp_path)
                .await
                .map_err(Error::OpenFile)?;

            client
                .get_object()
                .bucket(&*bucket)
                .key(&format!("{}/{}", dir_name, &file_name))
                .send()
                .await
                .map_err(Error::S3Get)?
                .body
                .map_err(|_| Error::S3GetObjChunk)
                .try_fold(file, |mut file, chunk| async move {
                    file.write_all(&chunk).await.map_err(Error::WriteFile)?;
                    Ok(file)
                })
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

async fn sync_files_to_s3(
    data_path: &Path,
    bucket: Arc<str>,
    client: Arc<aws_sdk_s3::Client>,
) -> Result<FileFutures> {
    let dir_names = DirName::list_sorted(data_path).await?;
    let s3_names = get_list(&bucket, &client).await?;

    let mut futs: FileFutures = Vec::new();

    for dir_name in dir_names {
        if dir_name.is_temp {
            continue;
        }

        for kind in ["block", "tx", "log"] {
            let mut path = data_path.to_owned();
            path.push(dir_name.to_string());
            path.push(format!("{kind}.parquet"));
            let s3_path = format!("{dir_name}/{kind}.parquet");
            if s3_names.contains(s3_path.as_str()) {
                continue;
            }
            let client = client.clone();
            let bucket = bucket.clone();
            let fut = async move {
                let file = aws_sdk_s3::types::ByteStream::read_from()
                    .path(&path)
                    .build()
                    .await
                    .unwrap();

                client
                    .put_object()
                    .bucket(&*bucket)
                    .key(&s3_path)
                    .body(file)
                    .send()
                    .await
                    .map_err(Error::S3Put)?;

                Ok(())
            };

            futs.push(Box::pin(fut));
        }
    }

    Ok(futs)
}

pub async fn get_list(bucket: &str, client: &aws_sdk_s3::Client) -> Result<BTreeSet<String>> {
    let mut s3_names = BTreeSet::new();
    let mut stream = client
        .list_objects_v2()
        .bucket(bucket)
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
