use arrow2::array::{Array, BooleanArray, MutableListArray, MutableUtf8Array, UInt64Array};
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

#[derive(Debug)]
pub struct Blocks {
    pub number: Vec<u64>,
    pub timestamp: Vec<u64>,
    pub nonce: MutableUtf8Array<i64>,
    pub size: MutableUtf8Array<i64>,
}

#[derive(Debug)]
pub struct Block {
    pub number: u64,
    pub timestamp: u64,
    pub nonce: String,
    pub size: String,
}

type RowGroups =
    RowGroupIterator<Arc<dyn Array>, std::vec::IntoIter<Result<Chunk<Arc<dyn Array>>, ArrowError>>>;

impl IntoRowGroups for Blocks {
    type Elem = Block;

    fn into_row_groups(self) -> (RowGroups, Schema, WriteOptions) {
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

    fn push(&mut self, elem: Self::Elem) {
        self.number.push(elem.number);
        self.timestamp.push(elem.timestamp);
        self.nonce.push(Some(elem.nonce));
        self.size.push(Some(elem.size));
    }
}

#[derive(Debug)]
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

impl IntoRowGroups for Transactions {
    type Elem = String;

    fn into_row_groups(self) -> (RowGroups, Schema, WriteOptions) {
        let chunk = Chunk::new(vec![
            self.hash.into_arc(),
            self.nonce.into_arc(),
            self.block_hash.into_arc(),
            Arc::new(UInt64Array::from(self.block_number.as_slice())) as Arc<dyn Array>,
            self.transaction_index.into_arc(),
            self.from.into_arc(),
            self.to.into_arc(),
            self.value.into_arc(),
            self.gas_price.into_arc(),
            self.gas.into_arc(),
            self.input.into_arc(),
            self.public_key.into_arc(),
            self.chain_id.into_arc(),
        ]);

        let schema = transaction_schema();

        let row_groups = RowGroupIterator::try_new(
            vec![Ok(chunk)].into_iter(),
            &schema,
            options(),
            vec![
                Encoding::DeltaLengthByteArray,
                Encoding::DeltaLengthByteArray,
                Encoding::DeltaLengthByteArray,
                Encoding::Plain,
                Encoding::DeltaLengthByteArray,
                Encoding::DeltaLengthByteArray,
                Encoding::DeltaLengthByteArray,
                Encoding::DeltaLengthByteArray,
                Encoding::DeltaLengthByteArray,
                Encoding::DeltaLengthByteArray,
                Encoding::DeltaLengthByteArray,
                Encoding::DeltaLengthByteArray,
                Encoding::DeltaLengthByteArray,
            ],
        )
        .unwrap();

        (row_groups, schema, options())
    }

    fn push(&mut self, elem: Self::Elem) {
        unimplemented!();
    }
}

#[derive(Debug)]
pub struct Logs {
    pub removed: Vec<bool>,
    pub log_index: MutableUtf8Array<i64>,
    pub transaction_index: MutableUtf8Array<i64>,
    pub transaction_hash: MutableUtf8Array<i64>,
    pub block_hash: MutableUtf8Array<i64>,
    pub block_number: Vec<Option<u64>>,
    pub address: MutableUtf8Array<i64>,
    pub data: MutableUtf8Array<i64>,
    pub topics: MutableListArray<i64, MutableUtf8Array<i32>>,
}

impl IntoRowGroups for Logs {
    type Elem = String;

    fn into_row_groups(self) -> (RowGroups, Schema, WriteOptions) {
        let chunk = Chunk::new(vec![
            Arc::new(BooleanArray::from_slice(self.removed.as_slice())) as Arc<dyn Array>,
            self.log_index.into_arc(),
            self.transaction_index.into_arc(),
            self.transaction_hash.into_arc(),
            self.block_hash.into_arc(),
            Arc::new(UInt64Array::from(self.block_number.as_slice())) as Arc<dyn Array>,
            self.address.into_arc(),
            self.data.into_arc(),
            self.topics.into_arc(),
        ]);

        let schema = log_schema();

        let row_groups = RowGroupIterator::try_new(
            vec![Ok(chunk)].into_iter(),
            &schema,
            options(),
            vec![
                Encoding::DeltaLengthByteArray,
                Encoding::DeltaLengthByteArray,
                Encoding::DeltaLengthByteArray,
                Encoding::DeltaLengthByteArray,
                Encoding::DeltaLengthByteArray,
                Encoding::Plain,
                Encoding::DeltaLengthByteArray,
                Encoding::DeltaLengthByteArray,
                Encoding::DeltaLengthByteArray,
            ],
        )
        .unwrap();

        (row_groups, schema, options())
    }

    fn push(&mut self, elem: Self::Elem) {
        unimplemented!();
    }
}

pub trait IntoRowGroups {
    type Elem: Send + Sync + std::fmt::Debug + 'static + std::marker::Sized;

    fn into_row_groups(self) -> (RowGroups, Schema, WriteOptions);
    fn push(&mut self, elem: Self::Elem);
}
