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
use hex::FromHex;
use serde::{Deserialize, Serialize};
use std::result::Result as StdResult;
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

#[derive(Debug, Default)]
pub struct Blocks {
    pub number: UInt64Vec,
    pub timestamp: UInt64Vec,
    pub nonce: MutableUtf8Array<i64>,
    pub size: MutableUtf8Array<i64>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Block {
    #[serde(deserialize_with = "hex::serde::deserialize")]
    pub number: Num,
    #[serde(deserialize_with = "hex::serde::deserialize")]
    pub timestamp: Num,
    pub nonce: String,
    pub size: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Num(u64);

impl hex::FromHex for Num {
    type Error = hex::FromHexError;

    fn from_hex<T: AsRef<[u8]>>(hex: T) -> StdResult<Self, Self::Error> {
        let arr = <[u8; 8]>::from_hex(hex)?;

        Ok(Self(u64::from_le_bytes(arr)))
    }
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
            self.timestamp.into_arc(),
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

    fn push(&mut self, elem: Self::Elem) -> Result<()> {
        self.number.push(Some(elem.number.0));
        self.timestamp.push(Some(elem.timestamp.0));
        self.nonce.push(Some(elem.nonce));
        self.size.push(Some(elem.size));

        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct Transactions {
    pub hash: MutableUtf8Array<i64>,
    pub nonce: MutableUtf8Array<i64>,
    pub block_hash: MutableUtf8Array<i64>,
    pub block_number: UInt64Vec,
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

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Transaction {
    pub hash: String,
    pub nonce: String,
    pub block_hash: Option<String>,
    pub block_number: Option<String>,
    pub transaction_index: Option<String>,
    pub from: String,
    pub to: Option<String>,
    pub value: String,
    pub gas_price: String,
    pub gas: String,
    pub input: String,
    pub public_key: String,
    pub chain_id: Option<String>,
}

impl IntoRowGroups for Transactions {
    type Elem = Transaction;

    fn into_row_groups(self) -> (RowGroups, Schema, WriteOptions) {
        let chunk = Chunk::new(vec![
            self.hash.into_arc(),
            self.nonce.into_arc(),
            self.block_hash.into_arc(),
            self.block_number.into_arc(),
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

    fn push(&mut self, elem: Self::Elem) -> Result<()> {
        self.hash.push(Some(elem.hash));
        self.nonce.push(Some(elem.nonce));
        self.block_hash.push(elem.block_hash);
        self.block_number.push(match elem.block_number {
            Some(block_number) => {
                let block_number = <[u8; 8]>::from_hex(block_number).map_err(Error::Hex)?;
                Some(u64::from_le_bytes(block_number))
            }
            None => None,
        });
        self.transaction_index.push(elem.transaction_index);
        self.from.push(Some(elem.from));
        self.to.push(elem.to);
        self.value.push(Some(elem.value));
        self.gas_price.push(Some(elem.gas_price));
        self.gas.push(Some(elem.gas));
        self.input.push(Some(elem.input));
        self.public_key.push(Some(elem.public_key));
        self.chain_id.push(elem.chain_id);

        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct Logs {
    pub removed: MutableBooleanArray,
    pub log_index: MutableUtf8Array<i64>,
    pub transaction_index: MutableUtf8Array<i64>,
    pub transaction_hash: MutableUtf8Array<i64>,
    pub block_hash: MutableUtf8Array<i64>,
    pub block_number: UInt64Vec,
    pub address: MutableUtf8Array<i64>,
    pub data: MutableUtf8Array<i64>,
    pub topics: MutableListArray<i64, MutableUtf8Array<i32>>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Log {
    pub removed: bool,
    pub log_index: Option<String>,
    pub transaction_index: Option<String>,
    pub transaction_hash: Option<String>,
    pub block_hash: Option<String>,
    pub block_number: Option<String>,
    pub address: String,
    pub data: String,
    pub topics: Vec<String>,
}

impl IntoRowGroups for Logs {
    type Elem = Log;

    fn into_row_groups(self) -> (RowGroups, Schema, WriteOptions) {
        let chunk = Chunk::new(vec![
            self.removed.into_arc(),
            self.log_index.into_arc(),
            self.transaction_index.into_arc(),
            self.transaction_hash.into_arc(),
            self.block_hash.into_arc(),
            self.block_number.into_arc(),
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

    fn push(&mut self, elem: Self::Elem) -> Result<()> {
        self.removed.push(Some(elem.removed));
        self.log_index.push(elem.log_index);
        self.transaction_index.push(elem.transaction_index);
        self.transaction_hash.push(elem.transaction_hash);
        self.block_hash.push(elem.block_hash);
        self.block_number.push(match elem.block_number {
            Some(block_number) => {
                let block_number = <[u8; 8]>::from_hex(block_number).map_err(Error::Hex)?;
                Some(u64::from_le_bytes(block_number))
            }
            None => None,
        });
        self.address.push(Some(elem.address));
        self.data.push(Some(elem.data));
        self.topics
            .try_push(Some(elem.topics.into_iter().map(Some)))
            .map_err(Error::PushRow)?;

        Ok(())
    }
}

pub trait IntoRowGroups {
    type Elem: Send + Sync + std::fmt::Debug + 'static + std::marker::Sized;

    fn into_row_groups(self) -> (RowGroups, Schema, WriteOptions);
    fn push(&mut self, elem: Self::Elem) -> Result<()>;
}
