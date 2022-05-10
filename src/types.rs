use crate::{Error, Result};
use web3::types::{Block, Log, Transaction, H256};

pub trait WriteToParquet: Send + Sync + std::fmt::Debug + 'static + std::marker::Sized {
    fn write_to_row_group(chunk: Vec<Self>) -> Result<()>;
}

impl WriteToParquet for Block<H256> {
    fn write_to_row_group(chunk: Vec<Self>) -> Result<()> {
        unimplemented!();
    }
}

impl WriteToParquet for Transaction {
    fn write_to_row_group(chunk: Vec<Self>) -> Result<()> {
        unimplemented!();
    }
}

impl WriteToParquet for Log {
    fn write_to_row_group(chunk: Vec<Self>) -> Result<()> {
        unimplemented!();
    }
}
