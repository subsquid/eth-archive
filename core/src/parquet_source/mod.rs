use crate::types::{Block, FormatVersion, Log, Transaction};
use crate::{Error, Result};
use arrow2::datatypes::Field;
use arrow2::io::parquet::read::ArrayIter;
use arrow2::io::parquet::read::{read_columns_many, read_metadata};
use std::collections::BTreeMap;
use std::io::Cursor;

pub mod util;
mod ver0_0_39;
mod ver0_1_0;

pub fn get(ver: FormatVersion) -> Box<dyn ParquetSource> {
    match ver {
        FormatVersion::Ver0_0_39 => Box::new(ver0_0_39::Ver0_0_39),
        FormatVersion::Ver0_1_0 => Box::new(ver0_1_0::Ver0_1_0),
    }
}

pub type Columns = Vec<Vec<ArrayIter<'static>>>;

pub fn read_parquet_buf(buf: &[u8], fields: Vec<Field>) -> Result<Columns> {
    let mut cursor = Cursor::new(&buf);
    let metadata = read_metadata(&mut cursor).map_err(Error::ReadParquet)?;
    let columns = metadata
        .row_groups
        .into_iter()
        .map(move |row_group_meta| {
            read_columns_many(
                &mut cursor,
                &row_group_meta,
                fields.clone(),
                None,
                None,
                None,
            )
            .map_err(Error::ReadParquet)
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(columns)
}

pub trait ParquetSource {
    fn read_blocks(&self, columns: Columns) -> BTreeMap<u32, Block>;

    fn read_txs(&self, columns: Columns) -> Vec<Transaction>;

    fn read_logs(&self, columns: Columns) -> Vec<Log>;

    fn block_fields(&self) -> Vec<Field>;

    fn tx_fields(&self) -> Vec<Field>;

    fn log_fields(&self) -> Vec<Field>;
}
