use crate::Error;
use arrow2::array::{
    Int64Vec, MutableArray, MutableBinaryArray as ArrowMutableBinaryArray, MutableBooleanArray,
    UInt64Vec,
};
use arrow2::compute::sort::{lexsort_to_indices, sort_to_indices, SortColumn, SortOptions};
use arrow2::compute::take::take as arrow_take;
use arrow2::datatypes::{DataType, Field, Schema};
use eth_archive_core::types::{Block, Log, Transaction};
use parquet_writer::{Chunk, IntoRowGroups, Result};

type MutableBinaryArray = ArrowMutableBinaryArray<i64>;

fn bytes32() -> DataType {
    DataType::Binary
}

fn bloom_filter_bytes() -> DataType {
    DataType::Binary
}

fn address() -> DataType {
    DataType::Binary
}

fn block_schema() -> Schema {
    Schema::from(vec![
        Field::new("number", DataType::Int64, false),
        Field::new("hash", bytes32(), false),
        Field::new("parent_hash", bytes32(), false),
        Field::new("nonce", DataType::UInt64, false),
        Field::new("sha3_uncles", bytes32(), false),
        Field::new("logs_bloom", bloom_filter_bytes(), false),
        Field::new("transactions_root", bytes32(), false),
        Field::new("state_root", bytes32(), false),
        Field::new("receipts_root", bytes32(), false),
        Field::new("miner", address(), false),
        Field::new("difficulty", DataType::Binary, false),
        Field::new("total_difficulty", DataType::Binary, false),
        Field::new("extra_data", DataType::Binary, false),
        Field::new("size", DataType::Int64, false),
        Field::new("gas_limit", DataType::Binary, false),
        Field::new("gas_used", DataType::Binary, false),
        Field::new("timestamp", DataType::Int64, false),
    ])
}

fn transaction_schema() -> Schema {
    Schema::from(vec![
        Field::new("block_hash", bytes32(), false),
        Field::new("block_number", DataType::Int64, false),
        Field::new("source", address(), false),
        Field::new("gas", DataType::Int64, false),
        Field::new("gas_price", DataType::Int64, false),
        Field::new("hash", bytes32(), false),
        Field::new("input", DataType::Binary, false),
        Field::new("nonce", DataType::UInt64, false),
        Field::new("dest", address(), true),
        Field::new("transaction_index", DataType::Int64, false),
        Field::new("value", DataType::Binary, false),
        Field::new("kind", DataType::Int64, false),
        Field::new("chain_id", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
        Field::new("r", DataType::Binary, false),
        Field::new("s", DataType::Binary, false),
    ])
}

fn log_schema() -> Schema {
    Schema::from(vec![
        Field::new("address", address(), false),
        Field::new("block_hash", bytes32(), false),
        Field::new("block_number", DataType::Int64, false),
        Field::new("data", DataType::Binary, false),
        Field::new("log_index", DataType::Int64, false),
        Field::new("removed", DataType::Boolean, false),
        Field::new("topic0", bytes32(), true),
        Field::new("topic1", bytes32(), true),
        Field::new("topic2", bytes32(), true),
        Field::new("topic3", bytes32(), true),
        Field::new("transaction_hash", bytes32(), false),
        Field::new("transaction_index", DataType::Int64, false),
    ])
}

#[derive(Debug, Default)]
pub struct Blocks {
    pub number: Int64Vec,
    pub hash: MutableBinaryArray,
    pub parent_hash: MutableBinaryArray,
    pub nonce: UInt64Vec,
    pub sha3_uncles: MutableBinaryArray,
    pub logs_bloom: MutableBinaryArray,
    pub transactions_root: MutableBinaryArray,
    pub state_root: MutableBinaryArray,
    pub receipts_root: MutableBinaryArray,
    pub miner: MutableBinaryArray,
    pub difficulty: MutableBinaryArray,
    pub total_difficulty: MutableBinaryArray,
    pub extra_data: MutableBinaryArray,
    pub size: Int64Vec,
    pub gas_limit: MutableBinaryArray,
    pub gas_used: MutableBinaryArray,
    pub timestamp: Int64Vec,
    pub len: usize,
}

impl IntoRowGroups for Blocks {
    type Elem = Block;

    fn into_chunk(mut self) -> Chunk {
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

        Chunk::new(vec![
            arrow_take(number.as_ref(), &indices).unwrap(),
            arrow_take(self.hash.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.parent_hash.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.nonce.as_box().as_ref(), &indices).unwrap(),
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
            arrow_take(self.timestamp.as_box().as_ref(), &indices).unwrap(),
        ])
    }

    fn push(&mut self, elem: Self::Elem) -> Result<()> {
        self.number.push(Some(elem.number.0));
        self.hash.push(Some(elem.hash.0.as_slice()));
        self.parent_hash.push(Some(elem.parent_hash.0.as_slice()));
        self.nonce.push(Some(elem.nonce.0));
        self.sha3_uncles.push(Some(elem.sha3_uncles.0.as_slice()));
        self.logs_bloom.push(Some(elem.logs_bloom.0.as_slice()));
        self.transactions_root
            .push(Some(elem.transactions_root.0.as_slice()));
        self.state_root.push(Some(elem.state_root.0.as_slice()));
        self.receipts_root
            .push(Some(elem.receipts_root.0.as_slice()));
        self.miner.push(Some(elem.miner.0.as_slice()));
        self.difficulty.push(Some(elem.difficulty.0));
        self.total_difficulty.push(Some(elem.total_difficulty.0));
        self.extra_data.push(Some(elem.extra_data.0));
        self.size.push(Some(elem.size.0));
        self.gas_limit.push(Some(elem.gas_limit.0));
        self.gas_used.push(Some(elem.gas_used.0));
        self.timestamp.push(Some(elem.timestamp.0));

        self.len += 1;

        Ok(())
    }

    fn block_num(&self, elem: &Self::Elem) -> i64 {
        elem.number.0
    }

    fn len(&self) -> usize {
        self.len
    }

    fn schema() -> Schema {
        block_schema()
    }
}

#[derive(Debug, Default)]
pub struct Transactions {
    pub block_hash: MutableBinaryArray,
    pub block_number: Int64Vec,
    pub source: MutableBinaryArray,
    pub gas: Int64Vec,
    pub gas_price: Int64Vec,
    pub hash: MutableBinaryArray,
    pub input: MutableBinaryArray,
    pub nonce: UInt64Vec,
    pub dest: MutableBinaryArray,
    pub transaction_index: Int64Vec,
    pub value: MutableBinaryArray,
    pub kind: Int64Vec,
    pub chain_id: Int64Vec,
    pub v: Int64Vec,
    pub r: MutableBinaryArray,
    pub s: MutableBinaryArray,
    pub len: usize,
}

impl IntoRowGroups for Transactions {
    type Elem = Transaction;

    fn into_chunk(mut self) -> Chunk {
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

        Chunk::new(vec![
            arrow_take(self.block_hash.as_box().as_ref(), &indices).unwrap(),
            arrow_take(block_number.as_ref(), &indices).unwrap(),
            arrow_take(source.as_ref(), &indices).unwrap(),
            arrow_take(self.gas.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.gas_price.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.hash.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.input.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.nonce.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.dest.as_box().as_ref(), &indices).unwrap(),
            arrow_take(transaction_index.as_ref(), &indices).unwrap(),
            arrow_take(self.value.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.kind.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.chain_id.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.v.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.r.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.s.as_box().as_ref(), &indices).unwrap(),
        ])
    }

    fn push(&mut self, elem: Self::Elem) -> Result<()> {
        self.block_hash.push(Some(elem.block_hash.0.as_slice()));
        self.block_number.push(Some(elem.block_number.0));
        self.source.push(Some(elem.source.0.as_slice()));
        self.gas.push(Some(elem.gas.0));
        self.gas_price.push(Some(elem.gas_price.0));
        self.hash.push(Some(elem.hash.0.as_slice()));
        self.input.push(Some(elem.input.0));
        self.nonce.push(Some(elem.nonce.0));
        match elem.dest {
            Some(dest) => self.dest.push(Some(dest.0.as_slice())),
            None => self.dest.push::<&[u8]>(None),
        }
        self.transaction_index.push(Some(elem.transaction_index.0));
        self.value.push(Some(elem.value.0));
        self.kind.push(Some(elem.kind.0));
        self.chain_id.push(Some(elem.chain_id.0));
        self.v.push(Some(elem.v.0));
        self.r.push(Some(elem.r.0));
        self.s.push(Some(elem.s.0));

        self.len += 1;

        Ok(())
    }

    fn block_num(&self, elem: &Self::Elem) -> i64 {
        elem.block_number.0
    }

    fn len(&self) -> usize {
        self.len
    }

    fn schema() -> Schema {
        transaction_schema()
    }
}

#[derive(Debug, Default)]
pub struct Logs {
    pub address: MutableBinaryArray,
    pub block_hash: MutableBinaryArray,
    pub block_number: Int64Vec,
    pub data: MutableBinaryArray,
    pub log_index: Int64Vec,
    pub removed: MutableBooleanArray,
    pub topic0: MutableBinaryArray,
    pub topic1: MutableBinaryArray,
    pub topic2: MutableBinaryArray,
    pub topic3: MutableBinaryArray,
    pub transaction_hash: MutableBinaryArray,
    pub transaction_index: Int64Vec,
    pub len: usize,
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
                    values: block_number.as_ref(),
                    options: Some(SortOptions {
                        descending: false,
                        nulls_first: false,
                    }),
                },
                SortColumn {
                    values: address.as_ref(),
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
        self.address.push(Some(elem.address.0.as_slice()));
        self.block_hash.push(Some(elem.block_hash.0.as_slice()));
        self.block_number.push(Some(elem.block_number.0));
        self.data.push(Some(elem.data.0));
        self.log_index.push(Some(elem.log_index.0));
        self.removed.push(Some(elem.removed));
        self.topic0.push(elem.topics.get(0).map(|t| t.0.as_slice()));
        self.topic1.push(elem.topics.get(1).map(|t| t.0.as_slice()));
        self.topic2.push(elem.topics.get(2).map(|t| t.0.as_slice()));
        self.topic3.push(elem.topics.get(3).map(|t| t.0.as_slice()));
        self.transaction_hash
            .push(Some(elem.transaction_hash.0.as_slice()));
        self.transaction_index.push(Some(elem.transaction_index.0));

        self.len += 1;

        Ok(())
    }

    fn block_num(&self, elem: &Self::Elem) -> i64 {
        elem.block_number.0
    }

    fn len(&self) -> usize {
        self.len
    }

    fn schema() -> Schema {
        log_schema()
    }
}
