use crate::{Error, Result};
use arrow2::array::{
    Array, MutableArray, MutableBinaryArray as ArrowMutableBinaryArray, MutableBooleanArray,
    MutableFixedSizeBinaryArray, MutableUtf8Array, UInt64Vec,
};
use arrow2::chunk::Chunk as ArrowChunk;
use arrow2::compute::sort::{lexsort_to_indices, sort_to_indices, SortColumn, SortOptions};
use arrow2::compute::take::take as arrow_take;
use arrow2::datatypes::{DataType, Field, Schema};
use arrow2::error::ArrowError;
use arrow2::io::parquet::write::{
    CompressionOptions, Encoding, RowGroupIterator, Version, WriteOptions,
};
use eth_archive_core::types::{Block, Log, Transaction};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use serde::{Deserialize, Serialize};
use std::result::Result as StdResult;

type Chunk = ArrowChunk<Box<dyn Array>>;

type MutableBinaryArray = ArrowMutableBinaryArray<i64>;

fn bytes32() -> DataType {
    DataType::FixedSizeBinary(32)
}

fn bloom_filter_bytes() -> DataType {
    DataType::FixedSizeBinary(256)
}

fn address() -> DataType {
    DataType::FixedSizeBinary(20)
}

fn block_schema() -> Schema {
    Schema::from(vec![
        Field::new("number", DataType::UInt64, true),
        Field::new("hash", bytes32(), false),
        Field::new("parent_hash", bytes32(), false),
        Field::new("nonce", DataType::UInt64, false),
        Field::new("sha3_uncles", bytes32(), false),
        Field::new("logs_bloom", bloom_filter_bytes(), false),
        Field::new("transactions_root", bytes32(), false),
        Field::new("state_root", bytes32(), false),
        Field::new("receipts_root", bytes32(), false),
        Field::new("miner", address(), false),
        Field::new("difficulty", DataType::UInt64, false),
        Field::new("total_difficulty", DataType::UInt64, false),
        Field::new("extra_data", DataType::Binary, false),
        Field::new("size", DataType::UInt64, false),
        Field::new("gas_limit", DataType::UInt64, false),
        Field::new("gas_used", DataType::UInt64, false),
        Field::new("timestamp", DataType::UInt64, false),
    ])
}

fn transaction_schema() -> Schema {
    Schema::from(vec![
        Field::new("block_hash", bytes32(), true),
        Field::new("block_number", DataType::UInt64, false),
        Field::new("from", address(), false),
        Field::new("gas", DataType::UInt64, false),
        Field::new("gas_price", DataType::UInt64, false),
        Field::new("hash", bytes32(), false),
        Field::new("input", DataType::Binary, false),
        Field::new("nonce", DataType::UInt64, false),
        Field::new("to", address(), true),
        Field::new("transaction_index", DataType::UInt64, false),
        Field::new("value", DataType::Binary, false),
        Field::new("v", DataType::UInt64, false),
        Field::new("r", DataType::Binary, false),
        Field::new("s", DataType::Binary, false),
    ])
}

fn log_schema() -> Schema {
    Schema::from(vec![
        Field::new("address", address(), false),
        Field::new("block_hash", bytes32(), false),
        Field::new("block_number", DataType::UInt64, false),
        Field::new("data", DataType::Binary, false),
        Field::new("log_index", DataType::UInt64, false),
        Field::new("removed", DataType::Boolean, false),
        Field::new("topic0", bytes32(), true),
        Field::new("topic1", bytes32(), true),
        Field::new("topic2", bytes32(), true),
        Field::new("topic3", bytes32(), true),
        Field::new("transaction_hash", bytes32(), false),
        Field::new("transaction_index", DataType::UInt64, false),
    ])
}

fn options() -> WriteOptions {
    WriteOptions {
        write_statistics: true,
        compression: CompressionOptions::Snappy,
        version: Version::V2,
    }
}

fn bytes32_arr() -> MutableFixedSizeBinaryArray {
    MutableFixedSizeBinaryArray::new(32)
}

fn bloom_filter_arr() -> MutableFixedSizeBinaryArray {
    MutableFixedSizeBinaryArray::new(256)
}

fn address_arr() -> MutableFixedSizeBinaryArray {
    MutableFixedSizeBinaryArray::new(20)
}

#[derive(Debug)]
pub struct Blocks {
    pub number: UInt64Vec,
    pub hash: MutableFixedSizeBinaryArray,
    pub parent_hash: MutableFixedSizeBinaryArray,
    pub nonce: UInt64Vec,
    pub sha3_uncles: MutableFixedSizeBinaryArray,
    pub logs_bloom: MutableFixedSizeBinaryArray,
    pub transactions_root: MutableFixedSizeBinaryArray,
    pub state_root: MutableFixedSizeBinaryArray,
    pub receipts_root: MutableFixedSizeBinaryArray,
    pub miner: MutableFixedSizeBinaryArray,
    pub difficulty: UInt64Vec,
    pub total_difficulty: UInt64Vec,
    pub extra_data: MutableBinaryArray,
    pub size: UInt64Vec,
    pub gas_limit: UInt64Vec,
    pub gas_used: UInt64Vec,
    pub timestamp: UInt64Vec,
    pub len: usize,
}

impl Default for Blocks {
    fn default() -> Self {
        Self {
            number: Default::default(),
            hash: bytes32_arr(),
            parent_hash: bytes32_arr(),
            nonce: Default::default(),
            sha3_uncles: bytes32_arr(),
            logs_bloom: bloom_filter_arr(),
            transactions_root: bytes32_arr(),
            state_root: bytes32_arr(),
            receipts_root: bytes32_arr(),
            miner: address_arr(),
            difficulty: Default::default(),
            total_difficulty: Default::default(),
            extra_data: Default::default(),
            size: Default::default(),
            gas_limit: Default::default(),
            gas_used: Default::default(),
            timestamp: Default::default(),
            len: 0,
        }
    }
}

type RowGroups = RowGroupIterator<Box<dyn Array>, std::vec::IntoIter<StdResult<Chunk, ArrowError>>>;

impl IntoRowGroups for Blocks {
    type Elem = Block;

    fn into_chunk(mut self) -> Chunk {
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

        Chunk::new(vec![
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
        ])
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

    fn encoding() -> Vec<Encoding> {
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
        ]
    }

    fn schema() -> Schema {
        block_schema()
    }
}

#[derive(Debug)]
pub struct Transactions {
    pub block_hash: MutableFixedSizeBinaryArray,
    pub block_number: UInt64Vec,
    pub from: MutableFixedSizeBinaryArray,
    pub gas: UInt64Vec,
    pub gas_price: UInt64Vec,
    pub hash: MutableFixedSizeBinaryArray,
    pub input: MutableBinaryArray,
    pub nonce: UInt64Vec,
    pub to: MutableFixedSizeBinaryArray,
    pub transaction_index: UInt64Vec,
    pub value: MutableBinaryArray,
    pub v: UInt64Vec,
    pub r: MutableBinaryArray,
    pub s: MutableBinaryArray,
    pub len: usize,
}

impl Default for Transactions {
    fn default() -> Transactions {
        Self {
            block_hash: bytes32_arr(),
            block_number: Default::default(),
            from: bytes32_arr(),
            gas: Default::default(),
            gas_price: Default::default(),
            hash: bytes32_arr(),
            input: Default::default(),
            nonce: Default::default(),
            to: bytes32_arr(),
            transaction_index: Default::default(),
            value: Default::default(),
            v: Default::default(),
            r: Default::default(),
            s: Default::default(),
            len: 0,
        }
    }
}

impl IntoRowGroups for Transactions {
    type Elem = Transaction;

    fn into_chunk(mut self) -> Chunk {
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

        Chunk::new(vec![
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
        ])
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

    fn encoding() -> Vec<Encoding> {
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
        ]
    }

    fn schema() -> Schema {
        transaction_schema()
    }
}

#[derive(Debug)]
pub struct Logs {
    pub address: MutableFixedSizeBinaryArray,
    pub block_hash: MutableFixedSizeBinaryArray,
    pub block_number: UInt64Vec,
    pub data: MutableBinaryArray,
    pub log_index: UInt64Vec,
    pub removed: MutableBooleanArray,
    pub topic0: MutableFixedSizeBinaryArray,
    pub topic1: MutableFixedSizeBinaryArray,
    pub topic2: MutableFixedSizeBinaryArray,
    pub topic3: MutableFixedSizeBinaryArray,
    pub transaction_hash: MutableFixedSizeBinaryArray,
    pub transaction_index: UInt64Vec,
    pub len: usize,
}

impl Default for Logs {
    fn default() -> Self {
        Self {
            address: bytes32_arr(),
            block_hash: bytes32_arr(),
            block_number: Default::default(),
            data: Default::default(),
            log_index: Default::default(),
            removed: Default::default(),
            topic0: bytes32_arr(),
            topic1: bytes32_arr(),
            topic2: bytes32_arr(),
            topic3: bytes32_arr(),
            transaction_hash: bytes32_arr(),
            transaction_index: Default::default(),
            len: 0,
        }
    }
}

impl IntoRowGroups for Logs {
    type Elem = Log;

    fn into_chunk(mut self) -> Chunk {
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

        Chunk::new(vec![
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
        ])
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

    fn encoding() -> Vec<Encoding> {
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
        ]
    }

    fn schema() -> Schema {
        log_schema()
    }
}

pub trait IntoRowGroups: Default + std::marker::Sized + Send + Sync {
    type Elem: Send + Sync + std::fmt::Debug + 'static + std::marker::Sized;

    fn encoding() -> Vec<Encoding>;
    fn schema() -> Schema;
    fn into_chunk(self) -> Chunk;
    fn into_row_groups(elems: Vec<Self>) -> (RowGroups, Schema, WriteOptions) {
        let row_groups = RowGroupIterator::try_new(
            elems
                .into_par_iter()
                .map(|elem| Ok(Self::into_chunk(elem)))
                .collect::<Vec<_>>()
                .into_iter(),
            &Self::schema(),
            options(),
            Self::encoding(),
        )
        .unwrap();

        (row_groups, Self::schema(), options())
    }
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
