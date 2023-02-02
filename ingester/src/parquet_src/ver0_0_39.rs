use super::Data;
use crate::{Error, Result};
use eth_archive_core::dir_name::DirName;
use eth_archive_core::s3_sync::{get_list, parse_s3_name};
use eth_archive_core::types::{Block, BlockRange, Log, Transaction};
use futures::{Stream, TryStreamExt};
use std::collections::BTreeMap;
use std::sync::Arc;

fn block_not_found_err(block_num: u32) -> Result<()> {
    Err(Error::BlockNotFoundInS3(block_num))
}

fn stream_batches(
    start_block: u32,
    s3_src_bucket: Arc<str>,
    client: Arc<aws_sdk_s3::Client>,
    dir_names: Vec<DirName>,
) -> impl Stream<Item = Result<(Vec<BlockRange>, Vec<Vec<Block>>, Vec<Vec<Log>>)>> {
    let mut block_num = start_block;

    async_stream::try_stream! {
        for dir_name in dir_names {
            // s3 files have a gap in them
            if dir_name.range.from > block_num {
                // This is a function a call to make the macro work
                block_not_found_err(block_num)?;
            }

            let block_fut = read_blocks(
                    dir_name,
                    s3_src_bucket.clone(),
                    client.clone(),
                );

            let tx_fut = read_txs(
                    dir_name,
                    s3_src_bucket.clone(),
                    client.clone(),
                );

            let log_fut = read_logs(
                    dir_name,
                    s3_src_bucket.clone(),
                    client.clone(),
                );

            let (mut blocks, txs, logs) = futures::future::try_join3(block_fut, tx_fut, log_fut).await?;

            let mut block_range = Default::default();

            for &num in blocks.keys() {
                block_range += BlockRange {
                    from: num,
                    to: num + 1,
                };
            }

            for tx in txs {
                blocks.get_mut(&tx.block_number.0).unwrap().transactions.push(tx);
            }

            let blocks = blocks.into_values().collect::<Vec<_>>();

            block_num = dir_name.range.to;

            yield (vec![block_range], vec![blocks], vec![logs]);
        }
    }
}

pub async fn execute(
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
        .filter(|(_, (val, dir_name))| *val == 3 && dir_name.range.to >= start_block)
        .map(|(_, (_, dir_name))| dir_name)
        .collect::<Vec<_>>();

    let data = stream_batches(start_block, s3_src_bucket, client, dir_names);

    Ok(Box::pin(data))
}

async fn read_blocks(
    dir_name: DirName,
    s3_src_bucket: Arc<str>,
    client: Arc<aws_sdk_s3::Client>,
) -> Result<BTreeMap<u32, Block>> {
    let key = format!("{dir_name}/block.parquet");
    let file = read_file_from_s3(&key, &s3_src_bucket, &client).await?;

    todo!()
}

async fn read_txs(
    dir_name: DirName,
    s3_src_bucket: Arc<str>,
    client: Arc<aws_sdk_s3::Client>,
) -> Result<Vec<Transaction>> {
    let key = format!("{dir_name}/tx.parquet");
    let file = read_file_from_s3(&key, &s3_src_bucket, &client).await?;

    todo!()
}

async fn read_logs(
    dir_name: DirName,
    s3_src_bucket: Arc<str>,
    client: Arc<aws_sdk_s3::Client>,
) -> Result<Vec<Log>> {
    let key = format!("{dir_name}/log.parquet");
    let file = read_file_from_s3(&key, &s3_src_bucket, &client).await?;

    todo!()
}

async fn read_file_from_s3(
    key: &str,
    s3_src_bucket: &str,
    client: &aws_sdk_s3::Client,
) -> Result<Vec<u8>> {
    let data = client
        .get_object()
        .bucket(s3_src_bucket)
        .key(key)
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
