use crate::Error;
use arrow2::array::{
    Array, MutableArray, MutableBinaryArray as ArrowMutableBinaryArray, MutableBooleanArray,
    UInt32Vec, UInt64Vec,
};
use arrow2::chunk::Chunk as ArrowChunk;
use arrow2::compute::sort::{lexsort_to_indices, sort_to_indices, SortColumn, SortOptions};
use arrow2::compute::take::take as arrow_take;
use arrow2::datatypes::{DataType, Field, Schema};
use arrow2::error::Result as ArrowResult;
use arrow2::io::parquet::write::{CompressionOptions, Version, WriteOptions};
use eth_archive_core::types::{Block, Log, Transaction};
use std::cmp;

type Chunk = ArrowChunk<Box<dyn Array>>;
type MutableBinaryArray = ArrowMutableBinaryArray<i64>;

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
        Field::new("timestamp", DataType::Binary, false),
        Field::new("extra_data", DataType::Binary, false),
        Field::new("mix_hash", DataType::Binary, true),
        Field::new("nonce", DataType::UInt64, true),
        Field::new("total_difficulty", DataType::Binary, true),
        Field::new("base_fee_per_gas", DataType::Binary, true),
        Field::new("size", DataType::Binary, false),
        Field::new("hash", DataType::Binary, true),
    ])
}

pub fn tx_schema() -> Schema {
    Schema::from(vec![
        Field::new("kind", DataType::UInt32, true),
        Field::new("nonce", DataType::UInt64, false),
        Field::new("dest", DataType::Binary, true),
        Field::new("gas", DataType::Binary, false),
        Field::new("value", DataType::Binary, false),
        Field::new("input", DataType::Binary, false),
        Field::new("max_priority_fee_per_gas", DataType::Binary, true),
        Field::new("max_fee_per_gas", DataType::Binary, true),
        Field::new("y_parity", DataType::UInt32, true),
        Field::new("chain_id", DataType::UInt32, true),
        Field::new("v", DataType::UInt64, true),
        Field::new("r", DataType::Binary, false),
        Field::new("s", DataType::Binary, false),
        Field::new("source", DataType::Binary, true),
        Field::new("block_hash", DataType::Binary, false),
        Field::new("block_number", DataType::UInt32, false),
        Field::new("transaction_index", DataType::UInt32, false),
        Field::new("gas_price", DataType::Binary, true),
        Field::new("hash", DataType::Binary, false),
        Field::new("status", DataType::UInt32, true),
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
        Field::new("removed", DataType::Boolean, true),
        Field::new("topic0", DataType::Binary, true),
        Field::new("topic1", DataType::Binary, true),
        Field::new("topic2", DataType::Binary, true),
        Field::new("topic3", DataType::Binary, true),
        Field::new("transaction_hash", DataType::Binary, false),
        Field::new("transaction_index", DataType::UInt32, false),
    ])
}

#[derive(Debug, Default)]
pub struct Blocks {
    pub parent_hash: MutableBinaryArray,
    pub sha3_uncles: MutableBinaryArray,
    pub miner: MutableBinaryArray,
    pub state_root: MutableBinaryArray,
    pub transactions_root: MutableBinaryArray,
    pub receipts_root: MutableBinaryArray,
    pub logs_bloom: MutableBinaryArray,
    pub difficulty: MutableBinaryArray,
    pub number: UInt32Vec,
    pub gas_limit: MutableBinaryArray,
    pub gas_used: MutableBinaryArray,
    pub timestamp: MutableBinaryArray,
    pub extra_data: MutableBinaryArray,
    pub mix_hash: MutableBinaryArray,
    pub nonce: UInt64Vec,
    pub total_difficulty: MutableBinaryArray,
    pub base_fee_per_gas: MutableBinaryArray,
    pub size: MutableBinaryArray,
    pub hash: MutableBinaryArray,
    pub len: usize,
}

impl IntoChunks for Blocks {
    fn into_chunks(mut self, items_per_chunk: usize) -> Vec<ArrowResult<Chunk>> {
        let number = self.number.as_box();

        let indices = sort_to_indices::<i64>(
            number.as_ref(),
            &SortOptions {
                descending: false,
                nulls_first: false,
            },
            None,
        )
        .map_err(Error::SortRowGroup)
        .unwrap();

        let chunk = Chunk::new(vec![
            arrow_take(self.parent_hash.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.sha3_uncles.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.miner.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.state_root.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.transactions_root.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.receipts_root.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.logs_bloom.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.difficulty.as_box().as_ref(), &indices).unwrap(),
            arrow_take(number.as_ref(), &indices).unwrap(),
            arrow_take(self.gas_limit.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.gas_used.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.timestamp.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.extra_data.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.mix_hash.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.nonce.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.total_difficulty.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.base_fee_per_gas.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.size.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.hash.as_box().as_ref(), &indices).unwrap(),
        ]);

        (0..self.len)
            .step_by(items_per_chunk)
            .map(|start| {
                let end = cmp::min(self.len, start + items_per_chunk);
                let length = end - start;
                Ok(Chunk::new(
                    chunk.iter().map(|arr| arr.sliced(start, length)).collect(),
                ))
            })
            .collect()
    }
}

impl Blocks {
    pub fn push(&mut self, elem: Block) {
        self.parent_hash.push(Some(elem.parent_hash.to_vec()));
        self.sha3_uncles.push(Some(elem.sha3_uncles.to_vec()));
        self.miner.push(Some(elem.miner.to_vec()));
        self.state_root.push(Some(elem.state_root.to_vec()));
        self.transactions_root
            .push(Some(elem.transactions_root.to_vec()));
        self.receipts_root.push(Some(elem.receipts_root.to_vec()));
        self.logs_bloom.push(Some(elem.logs_bloom.to_vec()));
        self.difficulty.push(elem.difficulty.map(|n| n.0));
        self.number.push(Some(elem.number.0));
        self.gas_limit.push(Some(elem.gas_limit.0));
        self.gas_used.push(Some(elem.gas_used.0));
        self.timestamp.push(Some(elem.timestamp.0));
        self.extra_data.push(Some(elem.extra_data.0));
        self.mix_hash.push(elem.mix_hash.map(|n| n.to_vec()));
        self.nonce.push(elem.nonce.map(|n| n.0));
        self.total_difficulty
            .push(elem.total_difficulty.map(|n| n.0));
        self.base_fee_per_gas
            .push(elem.base_fee_per_gas.map(|n| n.0));
        self.size.push(Some(elem.size.0));
        self.hash.push(elem.hash.map(|n| n.to_vec()));

        self.len += 1;
    }
}

#[derive(Debug, Default)]
pub struct Transactions {
    pub kind: UInt32Vec,
    pub nonce: UInt64Vec,
    pub dest: MutableBinaryArray,
    pub gas: MutableBinaryArray,
    pub value: MutableBinaryArray,
    pub input: MutableBinaryArray,
    pub max_priority_fee_per_gas: MutableBinaryArray,
    pub max_fee_per_gas: MutableBinaryArray,
    pub y_parity: UInt32Vec,
    pub chain_id: UInt32Vec,
    pub v: UInt64Vec,
    pub r: MutableBinaryArray,
    pub s: MutableBinaryArray,
    pub source: MutableBinaryArray,
    pub block_hash: MutableBinaryArray,
    pub block_number: UInt32Vec,
    pub transaction_index: UInt32Vec,
    pub gas_price: MutableBinaryArray,
    pub hash: MutableBinaryArray,
    pub status: UInt32Vec,
    pub sighash: MutableBinaryArray,
    pub len: usize,
}

impl IntoChunks for Transactions {
    fn into_chunks(mut self, items_per_chunk: usize) -> Vec<ArrowResult<Chunk>> {
        let block_number = self.block_number.as_box();
        let transaction_index = self.transaction_index.as_box();
        let source = self.source.as_box();

        let indices = lexsort_to_indices::<i64>(
            &[
                SortColumn {
                    values: block_number.as_ref(),
                    options: Some(SortOptions {
                        descending: false,
                        nulls_first: false,
                    }),
                },
                SortColumn {
                    values: transaction_index.as_ref(),
                    options: Some(SortOptions {
                        descending: false,
                        nulls_first: false,
                    }),
                },
            ],
            None,
        )
        .map_err(Error::SortRowGroup)
        .unwrap();

        let chunk = Chunk::new(vec![
            arrow_take(self.kind.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.nonce.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.dest.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.gas.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.value.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.input.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.max_priority_fee_per_gas.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.max_fee_per_gas.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.y_parity.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.chain_id.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.v.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.r.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.s.as_box().as_ref(), &indices).unwrap(),
            arrow_take(source.as_ref(), &indices).unwrap(),
            arrow_take(self.block_hash.as_box().as_ref(), &indices).unwrap(),
            arrow_take(block_number.as_ref(), &indices).unwrap(),
            arrow_take(transaction_index.as_ref(), &indices).unwrap(),
            arrow_take(self.gas_price.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.hash.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.status.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.sighash.as_box().as_ref(), &indices).unwrap(),
        ]);

        (0..self.len)
            .step_by(items_per_chunk)
            .map(|start| {
                let end = cmp::min(self.len, start + items_per_chunk);
                let length = end - start;
                Ok(Chunk::new(
                    chunk.iter().map(|arr| arr.sliced(start, length)).collect(),
                ))
            })
            .collect()
    }
}

impl Transactions {
    pub fn push(&mut self, elem: Transaction) {
        self.sighash.push(elem.input.get(..4));
        self.kind.push(elem.kind.map(|n| n.0));
        self.nonce.push(Some(elem.nonce.0));
        match elem.dest {
            Some(dest) => self.dest.push(Some(dest.to_vec())),
            None => self.dest.push::<&[u8]>(None),
        }
        self.gas.push(Some(elem.gas.0));
        self.value.push(Some(elem.value.0));
        self.input.push(Some(elem.input.0));
        self.max_priority_fee_per_gas
            .push(elem.max_priority_fee_per_gas.map(|n| n.0));
        self.max_fee_per_gas.push(elem.max_fee_per_gas.map(|n| n.0));
        self.y_parity.push(elem.y_parity.map(|n| n.0));
        self.chain_id.push(elem.chain_id.map(|n| n.0));
        self.v.push(elem.v.map(|n| n.0));
        self.r.push(Some(elem.r.0));
        self.s.push(Some(elem.s.0));
        self.source.push(elem.source.map(|n| n.to_vec()));
        self.block_hash.push(Some(elem.block_hash.to_vec()));
        self.block_number.push(Some(elem.block_number.0));
        self.transaction_index.push(Some(elem.transaction_index.0));
        self.gas_price.push(elem.gas_price.map(|n| n.0));
        self.hash.push(Some(elem.hash.to_vec()));
        self.status.push(elem.status.map(|n| n.0));

        self.len += 1;
    }
}

#[derive(Debug, Default)]
pub struct Logs {
    pub address: MutableBinaryArray,
    pub block_hash: MutableBinaryArray,
    pub block_number: UInt32Vec,
    pub data: MutableBinaryArray,
    pub log_index: UInt32Vec,
    pub removed: MutableBooleanArray,
    pub topic0: MutableBinaryArray,
    pub topic1: MutableBinaryArray,
    pub topic2: MutableBinaryArray,
    pub topic3: MutableBinaryArray,
    pub transaction_hash: MutableBinaryArray,
    pub transaction_index: UInt32Vec,
    pub len: usize,
}

impl IntoChunks for Logs {
    fn into_chunks(mut self, items_per_chunk: usize) -> Vec<ArrowResult<Chunk>> {
        let block_number = self.block_number.as_box();
        let transaction_index = self.transaction_index.as_box();
        let address = self.address.as_box();

        let indices = lexsort_to_indices::<i64>(
            &[
                SortColumn {
                    values: address.as_ref(),
                    options: Some(SortOptions {
                        descending: false,
                        nulls_first: false,
                    }),
                },
                SortColumn {
                    values: block_number.as_ref(),
                    options: Some(SortOptions {
                        descending: false,
                        nulls_first: false,
                    }),
                },
            ],
            None,
        )
        .map_err(Error::SortRowGroup)
        .unwrap();

        let chunk = Chunk::new(vec![
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
        ]);

        (0..self.len)
            .step_by(items_per_chunk)
            .map(|start| {
                let end = cmp::min(self.len, start + items_per_chunk);
                let length = end - start;
                Ok(Chunk::new(
                    chunk.iter().map(|arr| arr.sliced(start, length)).collect(),
                ))
            })
            .collect()
    }
}

impl Logs {
    pub fn push(&mut self, elem: Log) {
        self.address.push(Some(elem.address.to_vec()));
        self.block_hash.push(Some(elem.block_hash.to_vec()));
        self.block_number.push(Some(elem.block_number.0));
        self.data.push(Some(elem.data.0));
        self.log_index.push(Some(elem.log_index.0));
        self.removed.push(elem.removed);
        self.topic0.push(elem.topics.get(0).map(|t| t.to_vec()));
        self.topic1.push(elem.topics.get(1).map(|t| t.to_vec()));
        self.topic2.push(elem.topics.get(2).map(|t| t.to_vec()));
        self.topic3.push(elem.topics.get(3).map(|t| t.to_vec()));
        self.transaction_hash
            .push(Some(elem.transaction_hash.to_vec()));
        self.transaction_index.push(Some(elem.transaction_index.0));

        self.len += 1;
    }
}

pub fn parquet_write_options(page_size: Option<usize>) -> WriteOptions {
    WriteOptions {
        write_statistics: true,
        compression: CompressionOptions::Lz4Raw,
        version: Version::V2,
        data_pagesize_limit: page_size,
    }
}

pub trait IntoChunks {
    fn into_chunks(self, items_per_chunk: usize) -> Vec<ArrowResult<Chunk>>;
}
