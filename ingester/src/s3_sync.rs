use crate::{Error, Result};
use aws_smithy_http::endpoint::Endpoint;
use eth_archive_core::config::ParsedS3Config;
use eth_archive_core::dir_name::DirName;
use std::collections::HashSet;
use std::convert::TryInto;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use futures::StreamExt;

pub async fn start(data_path: &Path, config: &ParsedS3Config) -> Result<()> {
    let cfg = aws_config::from_env()
        .endpoint_resolver(Endpoint::immutable(config.s3_endpoint.clone().try_into().unwrap()))
        .region(aws_types::region::Region::new(config.s3_bucket_region.clone()))
        .load()
        .await;
    let client = aws_sdk_s3::Client::new(&cfg);

    let bucket = config.s3_bucket_name.clone();
    let data_path = data_path.to_owned();

    tokio::spawn(async move {
        loop {
            if let Err(e) = sync_files(&data_path, &bucket, &client).await {
                eprintln!("failed to sync files to s3:\n{}", e);
            }

            tokio::time::sleep(Duration::from_secs(300)).await;
        }
    });

    Ok(())
}

async fn sync_files(data_path: &Path, bucket: &str, client: &aws_sdk_s3::Client) -> Result<()> {
    let dir_names = DirName::list_sorted(data_path)
        .await
        .map_err(Error::ListParquetFolderNames)?;

    log::info!("starting s3 sync. {} folders to sync", dir_names.len());

    let mut s3_names = HashSet::new();

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

    let mut start = Instant::now();
    for (i, dir_name) in dir_names.iter().enumerate() {
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
                        let file = aws_sdk_s3::types::ByteStream::read_from().path(&path).build().await.unwrap();

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
            let percentage = (i+1) as f64 / dir_names.len() as f64 * 100.;
            log::info!("s3 sync progress: {}/{} {:.2}%", i + 1, dir_names.len(), percentage);
            start = Instant::now();
        }
    }

    Ok(())
}
