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

type RowGroups =
    RowGroupIterator<Arc<dyn Array>, std::vec::IntoIter<Result<Chunk<Arc<dyn Array>>, ArrowError>>>;

impl Blocks {
    pub fn into_row_groups(self) -> (RowGroups, Schema, WriteOptions) {
        let chunk = Chunk::new(vec![
            Arc::new(UInt64Array::from_vec(self.number)) as Arc<dyn Array>,
            Arc::new(UInt64Array::from_vec(self.timestamp)) as Arc<dyn Array>,
            self.nonce.into_arc(),
            self.size.into_arc(),
        ]);

        let schema = block_schema();

        let row_groups = RowGroupIterator::try_new(
            vec![Ok(chunk)].into_iter(),
            &schema,
            options(),
            vec![
                Encoding::Plain,
                Encoding::Plain,
                Encoding::DeltaLengthByteArray,
                Encoding::DeltaLengthByteArray,
            ],
        )
        .unwrap();

        (row_groups, schema, options())
    }
}

pub struct Transactions {
    pub hash: MutableUtf8Array<i64>,
    pub nonce: MutableUtf8Array<i64>,
    pub block_hash: MutableUtf8Array<i64>,
    pub block_number: Vec<Option<u64>>,
    pub transaction_index: MutableUtf8Array<i64>,
    pub from: MutableUtf8Array<i64>,
    pub to: MutableUtf8Array<i64>,
    pub value: MutableUtf8Array<i64>,
    pub gas_price: MutableUtf8Array<i64>,
    pub gas: MutableUtf8Array<i64>,
    pub input: MutableUtf8Array<i64>,
    pub public_key: MutableUtf8Array<i64>,
    pub chain_id: MutableUtf8Array<i64>,
}

pub struct Logs {
    pub removed: Vec<bool>,
    pub log_index: MutableUtf8Array<i64>,
    pub transaction_index: MutableUtf8Array<i64>,
    pub transaction_hash: MutableUtf8Array<i64>,
    pub block_hash: MutableUtf8Array<i64>,
    pub block_number: Vec<Option<u64>>,
    pub address: MutableUtf8Array<i64>,
    pub data: MutableUtf8Array<i64>,
    pub topics: MutableUtf8Array<i64>,
}
