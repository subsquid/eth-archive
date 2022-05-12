use arrow2::array::{Array, MutableUtf8Array, UInt64Array, Utf8Array};
use arrow2::chunk::Chunk;
use arrow2::datatypes::{DataType, Field, Schema};
use arrow2::error::ArrowError;
use arrow2::io::parquet::write::{
    CompressionOptions, Encoding, RowGroupIterator, Version, WriteOptions,
};
use std::sync::Arc;

fn block_schema() -> Schema {
    Schema::from(vec![
        Field::new("number", DataType::UInt64, false),
        Field::new("timestamp", DataType::UInt64, false),
        Field::new("nonce", DataType::Utf8, false),
        Field::new("size", DataType::Utf8, false),
    ])
}

fn transaction_schema() -> Schema {
    Schema::from(vec![
        Field::new("hash", DataType::Utf8, false),
        Field::new("nonce", DataType::Utf8, false),
        Field::new("block_hash", DataType::Utf8, true),
        Field::new("block_number", DataType::UInt64, true),
        Field::new("transaction_index", DataType::Utf8, true),
        Field::new("from", DataType::Utf8, false),
        Field::new("to", DataType::Utf8, true),
        Field::new("value", DataType::Utf8, false),
        Field::new("gas_price", DataType::Utf8, false),
        Field::new("gas", DataType::Utf8, false),
        Field::new("input", DataType::Utf8, false),
        Field::new("public_key", DataType::Utf8, false),
        Field::new("chain_id", DataType::Utf8, true),
    ])
}

fn log_schema() -> Schema {
    Schema::from(vec![
        Field::new("removed", DataType::Boolean, false),
        Field::new("log_index", DataType::Utf8, false),
        Field::new("transaction_index", DataType::Utf8, true),
        Field::new("transaction_hash", DataType::Utf8, true),
        Field::new("block_hash", DataType::Utf8, true),
        Field::new("block_number", DataType::UInt64, true),
        Field::new("address", DataType::Utf8, false),
        Field::new("data", DataType::Utf8, false),
        Field::new(
            "topics",
            DataType::List(Box::new(Field::new("topic", DataType::Utf8, false))),
            false,
        ),
    ])
}

fn options() -> WriteOptions {
    WriteOptions {
        write_statistics: true,
        compression: CompressionOptions::Snappy,
        version: Version::V1,
    }
}

pub struct Blocks {
    pub number: Vec<u64>,
    pub timestamp: Vec<u64>,
    pub nonce: MutableUtf8Array<i64>,
    pub size: MutableUtf8Array<i64>,
}

type RowGroup =
    RowGroupIterator<Arc<dyn Array>, std::vec::IntoIter<Result<Chunk<Arc<dyn Array>>, ArrowError>>>;

impl Blocks {
    pub fn into_row_group(self) -> Result<RowGroup, ArrowError> {
        let chunk = Chunk::new(vec![
            Arc::new(UInt64Array::from_vec(self.number)) as Arc<dyn Array>,
            Arc::new(UInt64Array::from_vec(self.timestamp)) as Arc<dyn Array>,
            self.nonce.into_arc(),
            self.size.into_arc(),
        ]);

        RowGroupIterator::try_new(
            vec![Ok(chunk)].into_iter(),
            &block_schema(),
            options(),
            vec![
                Encoding::Plain,
                Encoding::Plain,
                Encoding::DeltaLengthByteArray,
                Encoding::DeltaLengthByteArray,
            ],
        )
    }
}

pub struct Transactions {
    pub hash: Vec<String>,
    pub nonce: Vec<String>,
    pub block_hash: Vec<Option<String>>,
    pub block_number: Vec<Option<u64>>,
    pub transaction_index: Vec<Option<String>>,
    pub from: Vec<String>,
    pub to: Vec<Option<String>>,
    pub value: Vec<String>,
    pub gas_price: Vec<String>,
    pub gas: Vec<String>,
    pub input: Vec<String>,
    pub public_key: Vec<String>,
    pub chain_id: Vec<Option<String>>,
}

pub struct Logs {
    pub removed: Vec<bool>,
    pub log_index: Vec<String>,
    pub transaction_index: Vec<Option<String>>,
    pub transaction_hash: Vec<Option<String>>,
    pub block_hash: Vec<Option<String>>,
    pub block_number: Vec<Option<u64>>,
    pub address: Vec<String>,
    pub data: Vec<String>,
    pub topics: Vec<Vec<String>>,
}
