use crate::{Error, Result};
use arrow2::array::{Array, MutableArray, MutableBooleanArray, MutableUtf8Array, UInt64Vec};
use arrow2::chunk::Chunk;
use arrow2::compute::sort::{lexsort_to_indices, sort_to_indices, SortColumn, SortOptions};
use arrow2::compute::take::take as arrow_take;
use arrow2::datatypes::{DataType, Field, Schema};
use arrow2::error::ArrowError;
use arrow2::io::parquet::write::{
    CompressionOptions, Encoding, RowGroupIterator, Version, WriteOptions,
};
use serde::{Deserialize, Serialize};
use std::result::Result as StdResult;

fn block_schema() -> Schema {
    Schema::from(vec![
        Field::new("number", DataType::UInt64, true),
        Field::new("hash", DataType::Utf8, true),
        Field::new("parent_hash", DataType::Utf8, false),
        Field::new("nonce", DataType::Utf8, false),
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
    ])
}

fn transaction_schema() -> Schema {
    Schema::from(vec![
        Field::new("block_hash", DataType::Utf8, true),
        Field::new("block_number", DataType::UInt64, true),
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
        Field::new("block_number", DataType::UInt64, false),
        Field::new("data", DataType::Utf8, false),
        Field::new("log_index", DataType::Utf8, false),
        Field::new("removed", DataType::Boolean, false),
        Field::new("topic0", DataType::Utf8, true),
        Field::new("topic1", DataType::Utf8, true),
        Field::new("topic2", DataType::Utf8, true),
        Field::new("topic3", DataType::Utf8, true),
        Field::new("transaction_hash", DataType::Utf8, false),
        Field::new("transaction_index", DataType::Utf8, true),
    ])
}

fn options() -> WriteOptions {
    WriteOptions {
        write_statistics: true,
        compression: CompressionOptions::Snappy,
        version: Version::V2,
    }
}

#[derive(Debug, Default)]
pub struct Blocks {
    pub number: UInt64Vec,
    pub hash: MutableUtf8Array<i64>,
    pub parent_hash: MutableUtf8Array<i64>,
    pub nonce: MutableUtf8Array<i64>,
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
    pub len: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Block {
    pub number: Option<String>,
    pub hash: Option<String>,
    pub parent_hash: String,
    pub nonce: String,
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
}

type RowGroups = RowGroupIterator<
    Box<dyn Array>,
    std::vec::IntoIter<StdResult<Chunk<Box<dyn Array>>, ArrowError>>,
>;

impl IntoRowGroups for Blocks {
    type Elem = Block;

    fn into_row_groups(mut self) -> (RowGroups, Schema, WriteOptions) {
        let number = self.number.as_box();

        let indices = sort_to_indices::<i64>(
            number.as_ref(),
            &SortOptions {
                descending: false,
                nulls_first: true,
            },
            None,
        )
        .map_err(Error::SortRowGroup)
        .unwrap();

        let cols = vec![
            arrow_take(number.as_ref(), &indices).unwrap(),
            arrow_take(self.hash.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.parent_hash.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.nonce.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.timestamp.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.sha3_uncles.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.logs_bloom.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.transactions_root.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.state_root.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.receipts_root.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.miner.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.difficulty.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.total_difficulty.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.extra_data.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.size.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.gas_limit.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.gas_used.as_box().as_ref(), &indices).unwrap(),
        ];
        let chunks = (0..self.len())
            .step_by(500)
            .map(|start| {
                let cols = cols
                    .iter()
                    .map(|arr| {
                        let end = std::cmp::min(arr.len(), start + 500);
                        arr.slice(start, end)
                    })
                    .collect();

                Ok(Chunk::new(cols))
            })
            .collect::<Vec<_>>();

        let schema = block_schema();

        let row_groups = RowGroupIterator::try_new(
            chunks.into_iter(),
            &schema,
            options(),
            vec![
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
        self.number.push(elem.number.map(|s| get_u64_from_hex(&s)));
        self.hash.push(elem.hash);
        self.parent_hash.push(Some(elem.parent_hash));
        self.nonce.push(Some(elem.nonce));
        self.sha3_uncles.push(Some(elem.sha3_uncles));
        self.logs_bloom.push(Some(elem.logs_bloom));
        self.transactions_root.push(Some(elem.transactions_root));
        self.state_root.push(Some(elem.state_root));
        self.receipts_root.push(Some(elem.receipts_root));
        self.miner.push(elem.miner);
        self.difficulty.push(Some(elem.difficulty));
        self.total_difficulty.push(elem.total_difficulty);
        self.extra_data.push(Some(elem.extra_data));
        self.size.push(Some(elem.size));
        self.gas_limit.push(Some(elem.gas_limit));
        self.gas_used.push(Some(elem.gas_used));
        self.timestamp.push(Some(elem.timestamp));

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
    pub block_number: UInt64Vec,
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

    fn into_row_groups(mut self) -> (RowGroups, Schema, WriteOptions) {
        let block_number = self.block_number.as_box();
        let transaction_index = self.transaction_index.as_box();
        let from = self.from.as_box();

        let indices = lexsort_to_indices::<i64>(
            &[
                SortColumn {
                    values: block_number.as_ref(),
                    options: Some(SortOptions {
                        descending: false,
                        nulls_first: true,
                    }),
                },
                SortColumn {
                    values: transaction_index.as_ref(),
                    options: Some(SortOptions {
                        descending: false,
                        nulls_first: true,
                    }),
                },
            ],
            None,
        )
        .map_err(Error::SortRowGroup)
        .unwrap();

        let cols = vec![
            arrow_take(self.block_hash.as_box().as_ref(), &indices).unwrap(),
            arrow_take(block_number.as_ref(), &indices).unwrap(),
            arrow_take(from.as_ref(), &indices).unwrap(),
            arrow_take(self.gas.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.gas_price.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.hash.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.input.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.nonce.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.to.as_box().as_ref(), &indices).unwrap(),
            arrow_take(transaction_index.as_ref(), &indices).unwrap(),
            arrow_take(self.value.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.v.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.r.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.s.as_box().as_ref(), &indices).unwrap(),
        ];

        let chunks = (0..self.len())
            .step_by(50000)
            .map(|start| {
                let cols = cols
                    .iter()
                    .map(|arr| {
                        let end = std::cmp::min(arr.len(), start + 500);
                        arr.slice(start, end)
                    })
                    .collect();

                Ok(Chunk::new(cols))
            })
            .collect::<Vec<_>>();

        let schema = transaction_schema();

        let row_groups = RowGroupIterator::try_new(
            chunks.into_iter(),
            &schema,
            options(),
            vec![
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
                Encoding::DeltaLengthByteArray,
                Encoding::DeltaLengthByteArray,
                Encoding::DeltaLengthByteArray,
            ],
        )
        .unwrap();

        (row_groups, schema, options())
    }

    fn push(&mut self, elem: Self::Elem) -> Result<()> {
        self.block_hash.push(elem.block_hash);
        self.block_number
            .push(elem.block_number.map(|s| get_u64_from_hex(&s)));
        self.from.push(Some(elem.from));
        self.gas.push(Some(elem.gas));
        self.gas_price.push(Some(elem.gas_price));
        self.hash.push(Some(elem.hash));
        self.input.push(Some(elem.input));
        self.nonce.push(Some(elem.nonce));
        self.to.push(elem.to);
        self.transaction_index.push(elem.transaction_index);
        self.value.push(Some(elem.value));
        self.v.push(Some(elem.v));
        self.r.push(Some(elem.r));
        self.s.push(Some(elem.s));

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
    pub block_number: UInt64Vec,
    pub data: MutableUtf8Array<i64>,
    pub log_index: MutableUtf8Array<i64>,
    pub removed: MutableBooleanArray,
    pub topic0: MutableUtf8Array<i64>,
    pub topic1: MutableUtf8Array<i64>,
    pub topic2: MutableUtf8Array<i64>,
    pub topic3: MutableUtf8Array<i64>,
    pub transaction_hash: MutableUtf8Array<i64>,
    pub transaction_index: MutableUtf8Array<i64>,
    pub len: usize,
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

    fn into_row_groups(mut self) -> (RowGroups, Schema, WriteOptions) {
        let block_number = self.block_number.as_box();
        let transaction_index = self.transaction_index.as_box();
        let address = self.address.as_box();

        let indices = lexsort_to_indices::<i64>(
            &[
                SortColumn {
                    values: address.as_ref(),
                    options: Some(SortOptions {
                        descending: false,
                        nulls_first: true,
                    }),
                },
                SortColumn {
                    values: block_number.as_ref(),
                    options: Some(SortOptions {
                        descending: false,
                        nulls_first: true,
                    }),
                },
                SortColumn {
                    values: transaction_index.as_ref(),
                    options: Some(SortOptions {
                        descending: false,
                        nulls_first: true,
                    }),
                },
            ],
            None,
        )
        .map_err(Error::SortRowGroup)
        .unwrap();

        let cols = vec![
            arrow_take(address.as_ref(), &indices).unwrap(),
            arrow_take(self.block_hash.as_box().as_ref(), &indices).unwrap(),
            arrow_take(block_number.as_ref(), &indices).unwrap(),
            arrow_take(self.data.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.log_index.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.removed.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.topic0.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.topic1.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.topic2.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.topic3.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.transaction_hash.as_box().as_ref(), &indices).unwrap(),
            arrow_take(transaction_index.as_ref(), &indices).unwrap(),
        ];
        let chunks = (0..self.len())
            .step_by(50000)
            .map(|start| {
                let cols = cols
                    .iter()
                    .map(|arr| {
                        let end = std::cmp::min(arr.len(), start + 500);
                        arr.slice(start, end)
                    })
                    .collect();

                Ok(Chunk::new(cols))
            })
            .collect::<Vec<_>>();

        let schema = log_schema();

        let row_groups = RowGroupIterator::try_new(
            chunks.into_iter(),
            &schema,
            options(),
            vec![
                Encoding::DeltaLengthByteArray,
                Encoding::DeltaLengthByteArray,
                Encoding::Plain,
                Encoding::DeltaLengthByteArray,
                Encoding::DeltaLengthByteArray,
                Encoding::Plain,
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
        self.address.push(Some(elem.address));
        self.block_hash.push(Some(elem.block_hash));
        self.block_number
            .push(Some(get_u64_from_hex(&elem.block_number)));
        self.data.push(Some(elem.data));
        self.log_index.push(Some(elem.log_index));
        self.removed.push(Some(elem.removed));
        self.topic0.push(elem.topics.get(0));
        self.topic1.push(elem.topics.get(1));
        self.topic2.push(elem.topics.get(2));
        self.topic3.push(elem.topics.get(3));
        self.transaction_hash.push(Some(elem.transaction_hash));
        self.transaction_index.push(elem.transaction_index);

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
