use super::Data;
use crate::{Error, Result};
use arrow2::array::{self, Int64Array, UInt32Array, UInt64Array};
use arrow2::compute::concatenate::concatenate;
use arrow2::datatypes::{DataType, Field, Schema};
use arrow2::io::parquet::read::{read_columns_many, read_metadata};
use eth_archive_core::deserialize::{
    Address, BigUnsigned, BloomFilterBytes, Bytes, Bytes32, Index,
};
use eth_archive_core::dir_name::DirName;
use eth_archive_core::s3_sync::{get_list, parse_s3_name};
use eth_archive_core::types::{Block, BlockRange, Log, Transaction};
use futures::{Stream, TryStreamExt};
use polars::prelude::*;
use std::collections::BTreeMap;
use std::io::Cursor;
use std::sync::Arc;

type BinaryArray = array::BinaryArray<i64>;

// defines columns using the result frame and a list of column names
macro_rules! define_cols {
    ($columns:expr, $($name:ident, $arrow_type:ident),*) => {
        $(
            let arrays = $columns.next().unwrap().collect::<Vec<_>>();
            let arrays = arrays.into_iter().map(|a| a.unwrap()).collect::<Vec<_>>();
            let arrs = arrays.iter().map(|a| a.as_ref()).collect::<Vec<_>>();
            let array = concatenate(arrs.as_slice()).unwrap();
            let $name = array.as_any().downcast_ref::<$arrow_type>().unwrap();
        )*
    };
}

macro_rules! map_from_arrow {
    ($src_field:ident, $map_type:expr, $idx:expr) => {
        $map_type($src_field.get($idx).unwrap())
    };
}

macro_rules! map_from_arrow_opt {
    ($src_field:ident, $map_type:expr, $idx:expr) => {
        $src_field.get($idx).map($map_type)
    };
}

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

pub fn block_schema() -> Schema {
    Schema::from(vec![
        Field::new("parent_hash", DataType::Binary, false),
        Field::new("sha3_uncles", DataType::Binary, false),
        Field::new("miner", DataType::Binary, false),
        Field::new("state_root", DataType::Binary, false),
        Field::new("transactions_root", DataType::Binary, false),
        Field::new("receipts_root", DataType::Binary, false),
        Field::new("logs_bloom", DataType::Binary, false),
        Field::new("difficulty", DataType::Binary, true),
        Field::new("number", DataType::UInt32, false),
        Field::new("gas_limit", DataType::Binary, false),
        Field::new("gas_used", DataType::Binary, false),
        Field::new("timestamp", DataType::Int64, false),
        Field::new("extra_data", DataType::Binary, false),
        Field::new("mix_hash", DataType::Binary, true),
        Field::new("nonce", DataType::UInt64, true),
        Field::new("total_difficulty", DataType::Binary, true),
        Field::new("base_fee_per_gas", DataType::Binary, true),
        Field::new("size", DataType::Int64, false),
        Field::new("hash", DataType::Binary, true),
    ])
}

pub fn tx_schema() -> Schema {
    Schema::from(vec![
        Field::new("kind", DataType::UInt32, false),
        Field::new("nonce", DataType::UInt64, false),
        Field::new("dest", DataType::Binary, true),
        Field::new("gas", DataType::Int64, false),
        Field::new("value", DataType::Binary, false),
        Field::new("input", DataType::Binary, false),
        Field::new("max_priority_fee_per_gas", DataType::Int64, true),
        Field::new("max_fee_per_gas", DataType::Int64, true),
        Field::new("y_parity", DataType::UInt32, true),
        Field::new("chain_id", DataType::UInt32, true),
        Field::new("v", DataType::Int64, true),
        Field::new("r", DataType::Binary, false),
        Field::new("s", DataType::Binary, false),
        Field::new("source", DataType::Binary, true),
        Field::new("block_hash", DataType::Binary, false),
        Field::new("block_number", DataType::UInt32, false),
        Field::new("transaction_index", DataType::UInt32, false),
        Field::new("gas_price", DataType::Int64, true),
        Field::new("hash", DataType::Binary, false),
        Field::new("sighash", DataType::Binary, true),
    ])
}

pub fn log_schema() -> Schema {
    Schema::from(vec![
        Field::new("address", DataType::Binary, false),
        Field::new("block_hash", DataType::Binary, false),
        Field::new("block_number", DataType::UInt32, false),
        Field::new("data", DataType::Binary, false),
        Field::new("log_index", DataType::UInt32, false),
        Field::new("removed", DataType::Boolean, false),
        Field::new("topic0", DataType::Binary, true),
        Field::new("topic1", DataType::Binary, true),
        Field::new("topic2", DataType::Binary, true),
        Field::new("topic3", DataType::Binary, true),
        Field::new("transaction_hash", DataType::Binary, false),
        Field::new("transaction_index", DataType::UInt32, false),
    ])
}

fn i64_to_bytes(num: i64) -> Bytes {
    let bytes = num.to_le_bytes();
    let idx = bytes
        .iter()
        .enumerate()
        .find(|(_, b)| **b != 0)
        .map(|b| b.0)
        .unwrap_or(bytes.len() - 1);
    let bytes = &bytes[idx..];
    Bytes::new(bytes)
}

async fn read_blocks(
    dir_name: DirName,
    s3_src_bucket: Arc<str>,
    client: Arc<aws_sdk_s3::Client>,
) -> Result<BTreeMap<u32, Block>> {
    let key = format!("{dir_name}/block.parquet");
    let file = read_file_from_s3(&key, &s3_src_bucket, &client).await?;
    let file: Arc<[u8]> = file.into();
    let mut cursor = Cursor::new(file);
    let metadata = read_metadata(&mut cursor).map_err(Error::ReadParquet)?;

    let mut blocks = BTreeMap::new();

    for row_group_meta in metadata.row_groups.iter() {
        let mut columns = read_columns_many(
            &mut cursor,
            row_group_meta,
            block_schema().fields,
            None,
            None,
            None,
        )
        .map_err(Error::ReadParquet)?
        .into_iter();

        #[rustfmt::skip]
        define_cols!(
            columns,
            block_parent_hash, BinaryArray,
            block_sha3_uncles, BinaryArray,
            block_miner, BinaryArray,
            block_state_root, BinaryArray,
            block_transactions_root, BinaryArray,
            block_receipts_root, BinaryArray,
            block_logs_bloom, BinaryArray,
            block_difficulty, BinaryArray,
            block_number, UInt32Array,
            block_gas_limit, BinaryArray,
            block_gas_used, BinaryArray,
            block_timestamp, Int64Array,
            block_extra_data, BinaryArray,
            block_mix_hash, BinaryArray,
            block_nonce, UInt64Array,
            block_total_difficulty, BinaryArray,
            block_base_fee_per_gas, BinaryArray,
            block_size, Int64Array,
            block_hash, BinaryArray
        );

        let len = block_number.len();

        for i in 0..len {
            let number = map_from_arrow!(block_number, Index, i);

            blocks.insert(
                number.0,
                Block {
                    parent_hash: map_from_arrow!(block_parent_hash, Bytes32::new, i),
                    sha3_uncles: map_from_arrow!(block_sha3_uncles, Bytes32::new, i),
                    miner: map_from_arrow!(block_miner, Address::new, i),
                    state_root: map_from_arrow!(block_state_root, Bytes32::new, i),
                    transactions_root: map_from_arrow!(block_transactions_root, Bytes32::new, i),
                    receipts_root: map_from_arrow!(block_receipts_root, Bytes32::new, i),
                    logs_bloom: map_from_arrow!(block_logs_bloom, BloomFilterBytes::new, i),
                    difficulty: map_from_arrow_opt!(block_difficulty, Bytes::new, i),
                    number,
                    gas_limit: map_from_arrow!(block_gas_limit, Bytes::new, i),
                    gas_used: map_from_arrow!(block_gas_used, Bytes::new, i),
                    timestamp: map_from_arrow!(block_timestamp, i64_to_bytes, i),
                    extra_data: map_from_arrow!(block_extra_data, Bytes::new, i),
                    mix_hash: map_from_arrow_opt!(block_mix_hash, Bytes32::new, i),
                    nonce: map_from_arrow_opt!(block_nonce, BigUnsigned, i),
                    total_difficulty: map_from_arrow_opt!(block_total_difficulty, Bytes::new, i),
                    base_fee_per_gas: map_from_arrow_opt!(block_base_fee_per_gas, Bytes::new, i),
                    size: map_from_arrow!(block_size, i64_to_bytes, i),
                    hash: map_from_arrow_opt!(block_hash, Bytes32::new, i),
                    transactions: Vec::new(),
                },
            );
        }
    }

    Ok(blocks)
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
