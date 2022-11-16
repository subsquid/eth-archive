use crate::config::ParsedS3Config;
use crate::dir_name::DirName;
use crate::{Error, Result};
use aws_smithy_http::endpoint::Endpoint;
use futures::{StreamExt, TryStreamExt};
use std::collections::BTreeSet;
use std::convert::TryInto;
use std::io;
use std::path::Path;
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
        .endpoint_resolver(Endpoint::immutable(
            config.s3_endpoint.clone().try_into().unwrap(),
        ))
        .region(aws_types::region::Region::new(
            config.s3_bucket_region.clone(),
        ))
        .load()
        .await;
    let client = aws_sdk_s3::Client::new(&cfg);

    let bucket = config.s3_bucket_name.clone();
    let data_path = data_path.to_owned();

    let sync_interval = config.s3_sync_interval_secs;

    tokio::spawn(async move {
        let mut start_time = Instant::now();

        loop {
            log::info!("starting s3 sync.");

            let res = match direction {
                Direction::Up => sync_files_to_s3(&data_path, &bucket, &client).await,
                Direction::Down => sync_files_from_s3(&data_path, &bucket, &client).await,
            };

            let elapsed = start_time.elapsed().as_secs();

            if let Err(e) = res {
                log::error!("failed to execute s3 sync:\n{}", e);
            } else {
                log::info!("finished s3 sync in {} seconds.", elapsed);
            }

            if elapsed < sync_interval {
                tokio::time::sleep(Duration::from_secs(sync_interval - elapsed)).await;
            }
            start_time = Instant::now();
        }
    });

    Ok(())
}

async fn sync_files_from_s3(
    data_path: &Path,
    bucket: &str,
    client: &aws_sdk_s3::Client,
) -> Result<()> {
    let mut s3_names = Vec::new();

    for s3_name in get_list(bucket, client).await?.iter() {
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

        s3_names.push((dir_name, file_name));
    }

    log::info!("{} files to sync from s3", s3_names.len());

    let mut start_time = Instant::now();

    for (i, (dir_name, file_name)) in s3_names.iter().enumerate() {
        let temp_path = {
            let mut path = data_path.to_owned();
            path.push(
                DirName {
                    is_temp: true,
                    ..*dir_name
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
            .bucket(bucket)
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
            let mut path = data_path.to_owned();
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

        let mut temp_path = temp_path;
        assert!(temp_path.pop());
        tokio::fs::remove_dir_all(&temp_path)
            .await
            .map_err(Error::RemoveTempDir)?;

        if start_time.elapsed().as_secs() > 15 {
            let percentage = (i + 1) as f64 / s3_names.len() as f64 * 100.;
            log::info!(
                "s3 sync progress: {}/{} {:.2}%",
                i + 1,
                s3_names.len(),
                percentage
            );
            start_time = Instant::now();
        }
    }

    Ok(())
}

async fn sync_files_to_s3(
    data_path: &Path,
    bucket: &str,
    client: &aws_sdk_s3::Client,
) -> Result<()> {
    let dir_names = DirName::list_sorted(data_path).await?;

    log::info!("starting s3 sync. {} folders to sync", dir_names.len());

    let s3_names = get_list(bucket, client).await?;

    let mut start = Instant::now();
    for (i, dir_name) in dir_names.iter().enumerate() {
        if dir_name.is_temp {
            continue;
        }

        let futs = ["block", "tx", "log"]
            .into_iter()
            .map(|kind| {
                let mut path = data_path.to_owned();
                path.push(dir_name.to_string());
                path.push(format!("{}.parquet", kind));
                let s3_path = format!("{}/{}.parquet", dir_name, kind);
                let s3_names = s3_names.clone();
                async move {
                    if !s3_names.contains(s3_path.as_str()) {
                        let file = aws_sdk_s3::types::ByteStream::read_from()
                            .path(&path)
                            .build()
                            .await
                            .unwrap();

                        client
                            .put_object()
                            .bucket(bucket)
                            .key(&s3_path)
                            .body(file)
                            .send()
                            .await
                            .map_err(Error::S3Put)?;
                    }

                    Ok(())
                }
            })
            .collect::<Vec<_>>();

        futures::future::try_join_all(futs).await?;

        if start.elapsed().as_secs() > 15 {
            let percentage = (i + 1) as f64 / dir_names.len() as f64 * 100.;
            log::info!(
                "s3 sync progress: {}/{} {:.2}%",
                i + 1,
                dir_names.len(),
                percentage
            );
            start = Instant::now();
        }
    }

    Ok(())
}

async fn get_list(bucket: &str, client: &aws_sdk_s3::Client) -> Result<Arc<BTreeSet<String>>> {
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
    let s3_names = Arc::new(s3_names);

    Ok(s3_names)
}
