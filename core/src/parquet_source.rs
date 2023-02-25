use crate::types::{Block, FormatVersion, Log, Transaction};
use polars::export::arrow::datatypes::Field;
use polars::export::arrow::io::parquet::read::ArrayIter;
use std::collections::BTreeMap;

mod util;
mod ver0_0_39;
mod ver0_1_0;

pub fn get(ver: FormatVersion) -> Box<dyn ParquetSource> {
    match ver {
        FormatVersion::Ver0_0_39 => Box::new(ver0_0_39::Ver0_0_39),
        FormatVersion::Ver0_1_0 => Box::new(ver0_1_0::Ver0_1_0),
    }
}

pub type Columns = Vec<Vec<ArrayIter<'static>>>;

pub trait ParquetSource {
    fn read_blocks(&self, columns: Columns) -> BTreeMap<u32, Block>;

    fn read_txs(&self, columns: Columns) -> Vec<Transaction>;

    fn read_logs(&self, columns: Columns) -> Vec<Log>;

    fn block_fields(&self) -> Vec<Field>;

    fn tx_fields(&self) -> Vec<Field>;

    fn log_fields(&self) -> Vec<Field>;
}
