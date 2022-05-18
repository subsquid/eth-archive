use crate::{Error, Result};
use arrow2::array::{
    Array, MutableBooleanArray, MutableListArray, MutableUtf8Array, TryPush, UInt64Vec,
};
use arrow2::chunk::Chunk;
use arrow2::datatypes::{DataType, Field, Schema};
use arrow2::error::ArrowError;
use arrow2::io::parquet::write::{
    CompressionOptions, Encoding, RowGroupIterator, Version, WriteOptions,
};
use serde::{Deserialize, Serialize};
use std::result::Result as StdResult;
use std::sync::Arc;

fn block_schema() -> Schema {
    Schema::from(vec![
        Field::new("number", DataType::UInt64, true),
        Field::new("hash", DataType::Utf8, true),
        Field::new("parent_hash", DataType::Utf8, false),
        Field::new("nonce", DataType::Utf8, false),
        Field::new("timestamp", DataType::Utf8, false),
        Field::new("sha3_uncles", DataType::Utf8, false),
        Field::new("logs_bloom", DataType::Utf8, false),
        Field::new("transactions_root", DataType::Utf8, false),
        Field::new("state_root", DataType::Utf8, false),
        Field::new("receipts_root", DataType::Utf8, false),
        Field::new("miner", DataType::Utf8, true),
        Field::new("difficulty", DataType::Utf8, false),
        Field::new("total_difficulty", DataType::Utf8, true),
        Field::new("extra_data", DataType::Utf8, false),
        Field::new("size", DataType::Utf8, false),
        Field::new("gas_limit", DataType::Utf8, false),
        Field::new("gas_used", DataType::Utf8, false),
        Field::new("timestamp", DataType::Utf8, false),
        Field::new(
            "uncles",
            DataType::List(Box::new(Field::new("uncle", DataType::Utf8, false))),
            false,
        ),
    ])
}

fn transaction_schema() -> Schema {
    Schema::from(vec![
        Field::new("block_hash", DataType::Utf8, true),
        Field::new("block_number", DataType::Utf8, true),
        Field::new("from", DataType::Utf8, false),
        Field::new("gas", DataType::Utf8, false),
        Field::new("gas_price", DataType::Utf8, false),
        Field::new("hash", DataType::Utf8, false),
        Field::new("input", DataType::Utf8, false),
        Field::new("nonce", DataType::Utf8, false),
        Field::new("to", DataType::Utf8, true),
        Field::new("transaction_index", DataType::Utf8, true),
        Field::new("value", DataType::Utf8, false),
        Field::new("v", DataType::Utf8, false),
        Field::new("r", DataType::Utf8, false),
        Field::new("s", DataType::Utf8, false),
    ])
}

fn log_schema() -> Schema {
    Schema::from(vec![
        Field::new("address", DataType::Utf8, false),
        Field::new("block_hash", DataType::Utf8, false),
        Field::new("block_number", DataType::Utf8, false),
        Field::new("data", DataType::Utf8, false),
        Field::new("log_index", DataType::Utf8, false),
        Field::new("removed", DataType::Bool, false),
        Field::new(
            "topics",
            DataType::List(Box::new(Field::new("topic", DataType::Utf8, false))),
            false,
        ),
        Field::new("transaction_hash", DataType::Utf8, false),
        Field::new("transaction_index", DataType::Utf8, true),
    ])
}

fn options() -> WriteOptions {
    WriteOptions {
        write_statistics: true,
        compression: CompressionOptions::Snappy,
        version: Version::V1,
    }
}

#[derive(Debug, Default)]
pub struct Blocks {
    pub number: UInt64Vec,
    pub hash: MutableUtf8Array<i64>,
    pub parent_hash: MutableUtf8Array<i64>,
    pub nonce: MutableUtf8Array<i64>,
    pub timestamp: MutableUtf8Array<i64>,
    pub sha3_uncles: MutableUtf8Array<i64>,
    pub logs_bloom: MutableUtf8Array<i64>,
    pub transactions_root: MutableUtf8Array<i64>,
    pub state_root: MutableUtf8Array<i64>,
    pub receipts_root: MutableUtf8Array<i64>,
    pub miner: MutableUtf8Array<i64>,
    pub difficulty: MutableUtf8Array<i64>,
    pub total_difficulty: MutableUtf8Array<i64>,
    pub extra_data: MutableUtf8Array<i64>,
    pub size: MutableUtf8Array<i64>,
    pub gas_limit: MutableUtf8Array<i64>,
    pub gas_used: MutableUtf8Array<i64>,
    pub timestamp: MutableUtf8Array<i64>,
    pub uncles: MutableListArray<i64, MutableUtf8Array<i32>>,
    pub len: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Block {
    pub number: Option<String>,
    pub hash: Option<String>,
    pub parent_hash: String,
    pub nonce: String,
    pub timestamp: String,
    pub sha3_uncles: String,
    pub logs_bloom: String,
    pub transactions_root: String,
    pub state_root: String,
    pub receipts_root: String,
    pub miner: Option<String>,
    pub difficulty: String,
    pub total_difficulty: Option<String>,
    pub extra_data: String,
    pub size: String,
    pub gas_limit: String,
    pub gas_used: String,
    pub timestamp: String,
    pub transactions: Vec<Transaction>,
    pub uncles: Vec<String>,
}

type RowGroups = RowGroupIterator<
    Arc<dyn Array>,
    std::vec::IntoIter<StdResult<Chunk<Arc<dyn Array>>, ArrowError>>,
>;

impl IntoRowGroups for Blocks {
    type Elem = Block;

    fn into_row_groups(self) -> (RowGroups, Schema, WriteOptions) {
        let chunk = Chunk::new(vec![
            self.number.into_arc(),
            self.hash.into_arc(),
            self.parent_hash.into_arc(),
            self.nonce.into_arc(),
            self.timestamp.into_arc(),
            self.sha3_uncles.into_arc(),
            self.logs_bloom.into_arc(),
            self.transactions_root.into_arc(),
            self.state_root.into_arc(),
            self.receipts_root.into_arc(),
            self.miner.into_arc(),
            self.difficulty.into_arc(),
            self.total_difficulty.into_arc(),
            self.extra_data.into_arc(),
            self.size.into_arc(),
            self.gas_limit.into_arc(),
            self.gas_used.into_arc(),
            self.timestamp.into_arc(),
            self.uncles.into_arc(),
        ]);

        let schema = block_schema();

        let row_groups = RowGroupIterator::try_new(
            vec![Ok(chunk)].into_iter(),
            &schema,
            options(),
            vec![
                Encoding::Plain,
                Encoding::Plain,
                Encoding::Plain,
                Encoding::Plain,
                Encoding::Plain,
                Encoding::Plain,
                Encoding::Plain,
                Encoding::Plain,
                Encoding::Plain,
                Encoding::Plain,
                Encoding::Plain,
                Encoding::Plain,
                Encoding::Plain,
                Encoding::Plain,
                Encoding::Plain,
                Encoding::Plain,
                Encoding::Plain,
                Encoding::Plain,
                Encoding::Plain,
                Encoding::Plain,
            ],
        )
        .unwrap();

        (row_groups, schema, options())
    }

    fn push(&mut self, elem: Self::Elem) -> Result<()> {
        self.number.push(elem.number),
        self.hash.push(elem.hash),
        self.parent_hash.push(Some(elem.parent_hash)),
        self.nonce.push(Some(elem.nonce)),
        self.timestamp.push(Some(elem.timestamp)),
        self.sha3_uncles.push(Some(elem.sha3_uncles)),
        self.logs_bloom.push(Some(elem.logs_bloom)),
        self.transactions_root.push(Some(elem.transactions_root)),
        self.state_root.push(Some(elem.state_root)),
        self.receipts_root.push(Some(elem.receipts_root)),
        self.miner.push(elem.miner),
        self.difficulty.push(Some(elem.difficulty)),
        self.total_difficulty.push(elem.total_difficulty),
        self.extra_data.push(Some(elem.extra_data)),
        self.size.push(Some(elem.size)),
        self.gas_limit.push(Some(elem.gas_limit)),
        self.gas_used.push(Some(elem.gas_used)),
        self.timestamp.push(Some(elem.timestamp)),
        self.uncles
            .try_push(Some(elem.uncles.into_iter().map(Some)))
            .map_err(Error::PushRow)?;

        self.len += 1;

        Ok(())
    }

    fn len(&self) -> usize {
        self.len
    }
}

#[derive(Debug, Default)]
pub struct Transactions {
    pub block_hash: MutableUtf8Array<i64>,
    pub block_number: MutableUtf8Array<i64>,
    pub from: MutableUtf8Array<i64>,
    pub gas: MutableUtf8Array<i64>,
    pub gas_price: MutableUtf8Array<i64>,
    pub hash: MutableUtf8Array<i64>,
    pub input: MutableUtf8Array<i64>,
    pub nonce: MutableUtf8Array<i64>,
    pub to: MutableUtf8Array<i64>,
    pub transaction_index: MutableUtf8Array<i64>,
    pub value: MutableUtf8Array<i64>,
    pub v: MutableUtf8Array<i64>,
    pub r: MutableUtf8Array<i64>,
    pub s: MutableUtf8Array<i64>,
    pub len: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Transaction {
    pub block_hash: Option<String>,
    pub block_number: Option<String>,
    pub from: String,
    pub gas: String,
    pub gas_price: String,
    pub hash: String,
    pub input: String,
    pub nonce: String,
    pub to: Option<String>,
    pub transaction_index: Option<String>,
    pub value: String,
    pub v: String,
    pub r: String,
    pub s: String,
}

impl IntoRowGroups for Transactions {
    type Elem = Transaction;

    fn into_row_groups(self) -> (RowGroups, Schema, WriteOptions) {
        let chunk = Chunk::new(vec![
            self.block_hash.into_arc(),
            self.block_number.into_arc(),
            self.from.into_arc(),
            self.gas.into_arc(),
            self.gas_price.into_arc(),
            self.hash.into_arc(),
            self.input.into_arc(),
            self.nonce.into_arc(),
            self.to.into_arc(),
            self.transaction_index.into_arc(),
            self.value.into_arc(),
            self.v.into_arc(),
            self.r.into_arc(),
            self.s.into_arc(),
        ]);

        let schema = transaction_schema();

        let row_groups = RowGroupIterator::try_new(
            vec![Ok(chunk)].into_iter(),
            &schema,
            options(),
            vec![
                Encoding::Plain,
                Encoding::Plain,
                Encoding::Plain,
                Encoding::Plain,
                Encoding::Plain,
                Encoding::Plain,
                Encoding::Plain,
                Encoding::Plain,
                Encoding::Plain,
                Encoding::Plain,
                Encoding::Plain,
                Encoding::Plain,
                Encoding::Plain,
                Encoding::Plain,
            ],
        )
        .unwrap();

        (row_groups, schema, options())
    }

    fn push(&mut self, elem: Self::Elem) -> Result<()> {
        self.block_hash.push(elem.block_hash),
        self.block_number.push(elem.block_number),
        self.from.push(Some(elem.from)),
        self.gas.push(Some(elem.gas)),
        self.gas_price.push(Some(elem.gas_price)),
        self.hash.push(Some(elem.hash)),
        self.input.push(Some(elem.input)),
        self.nonce.push(Some(elem.nonce)),
        self.to.push(elem.to),
        self.transaction_index.push(elem.transaction_index),
        self.value.push(Some(elem.value)),
        self.v.push(Some(elem.v)),
        self.r.push(Some(elem.r)),
        self.s.push(Some(elem.s)),

        self.len += 1;

        Ok(())
    }

    fn len(&self) -> usize {
        self.len
    }
}

#[derive(Debug, Default)]
pub struct Logs {
    pub address: MutableUtf8Array<i64>,
    pub block_hash: MutableUtf8Array<i64>,
    pub block_number: MutableUtf8Array<i64>,
    pub data: MutableUtf8Array<i64>,
    pub log_index: MutableUtf8Array<i64>,
    pub removed: MutableBooleanArray,
    pub topics: MutableListArray<i64, MutableUtf8Array<i32>>,
    pub transaction_hash: MutableUtf8Array<i64>,
    pub transaction_index: MutableUtf8Array<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Log {
    pub address: String,
    pub block_hash: String,
    pub block_number: String,
    pub data: String,
    pub log_index: String,
    pub removed: bool,
    pub topics: Vec<String>,
    pub transaction_hash: String,
    pub transaction_index: Option<String>,
}

impl IntoRowGroups for Logs {
    type Elem = Log;

    fn into_row_groups(self) -> (RowGroups, Schema, WriteOptions) {
        let chunk = Chunk::new(vec![
            self.address.into_arc(),
            self.block_hash.into_arc(),
            self.block_number.into_arc(),
            self.data.into_arc(),
            self.log_index.into_arc(),
            self.removed.into_arc(),
            self.topics.into_arc(),
            self.transaction_hash.into_arc(),
            self.transaction_index.into_arc(),
        ]);

        let schema = log_schema();

        let row_groups = RowGroupIterator::try_new(
            vec![Ok(chunk)].into_iter(),
            &schema,
            options(),
            vec![
                Encoding::Plain,
                Encoding::Plain,
                Encoding::Plain,
                Encoding::Plain,
                Encoding::Plain,
                Encoding::Plain,
                Encoding::Plain,
                Encoding::Plain,
                Encoding::Plain,
            ],
        )
        .unwrap();

        (row_groups, schema, options())
    }

    fn push(&mut self, elem: Self::Elem) -> Result<()> {
        self.address.push(Some(elem.address)),
        self.block_hash.push(Some(elem.block_hash)),
        self.block_number.push(Some(elem.block_number)),
        self.data.push(Some(elem.data)),
        self.log_index.push(Some(elem.log_index)),
        self.removed.push(Some(elem.removed)),
        self.topics
            .try_push(Some(elem.topics.into_iter().map(Some)))
            .map_err(Error::PushRow)?;
        self.transaction_hash.push(Some(elem.transaction_hash)),
        self.transaction_index.push(elem.transaction_index),
        
        self.len += 1;

        Ok(())
    }

    fn len(&self) -> usize {
        self.len
    }
}

pub trait IntoRowGroups: Default {
    type Elem: Send + Sync + std::fmt::Debug + 'static + std::marker::Sized;

    fn into_row_groups(self) -> (RowGroups, Schema, WriteOptions);
    fn push(&mut self, elem: Self::Elem) -> Result<()>;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

fn get_u64_from_hex(hex: &str) -> u64 {
    let without_prefix = hex.trim_start_matches("0x");
    u64::from_str_radix(without_prefix, 16).unwrap()
}
