use crate::{Error, Result};
use parquet::file::writer::RowGroupWriter;
use web3::types::{Address, Block, Bytes, Index, Log, Transaction, H160, H256, H64, U256, U64};

#[derive(Debug)]
pub struct Blocks {
    pub number: Vec<Option<U64>>,
    pub hash: Vec<Option<H256>>,
    pub parent_hash: Vec<H256>,
    pub uncles_hash: Vec<H256>,
    pub author: Vec<H160>,
    pub timestamp: Vec<U256>,
    pub size: Vec<Option<U256>>,
    pub nonce: Vec<Option<H64>>,
}

#[derive(Debug)]
pub struct Transactions {
    pub block_number: Vec<Option<U64>>,
    pub transaction_index: Vec<Option<Index>>,
    pub hash: Vec<H256>,
    pub nonce: Vec<U256>,
    pub block_hash: Vec<Option<H256>>,
    pub from: Vec<Option<Address>>,
    pub to: Vec<Option<Address>>,
    pub value: Vec<U256>,
    pub input: Vec<Bytes>,
    pub transaction_type: Vec<Option<U64>>,
}

#[derive(Debug)]
pub struct Logs {
    block_number: Vec<Option<U64>>,
    address: Vec<H160>,
    topics: Vec<Vec<H256>>,
    data: Vec<Bytes>,
    block_hash: Vec<Option<H256>>,
    transaction_index: Vec<Option<Index>>,
    transaction_hash: Vec<Option<H256>>,
    log_index: Vec<Option<U256>>,
    transaction_log_index: Vec<Option<U256>>,
}

pub trait WriteToParquet: Send + Sync + std::fmt::Debug + 'static + std::marker::Sized {
    fn write_to_row_group(&self, writer: &mut Box<dyn RowGroupWriter>) -> Result<()>;
}

impl WriteToParquet for Blocks {
    fn write_to_row_group(&self, writer: &mut Box<dyn RowGroupWriter>) -> Result<()> {
        unimplemented!();
    }
}

impl WriteToParquet for Transactions {
    fn write_to_row_group(&self, writer: &mut Box<dyn RowGroupWriter>) -> Result<()> {
        unimplemented!();
    }
}

impl WriteToParquet for Logs {
    fn write_to_row_group(&self, writer: &mut Box<dyn RowGroupWriter>) -> Result<()> {
        unimplemented!();
    }
}
