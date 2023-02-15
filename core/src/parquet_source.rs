mod util;
mod ver0_0_39;

pub fn get(ver: FormatVersion) -> Box<dyn ParquetSource> {
    match ver {
        FormatVersion::Ver0_0_39 => Box::new(ver0_0_39::Ver0_0_39),
    }
}

trait ParquetSource {
    fn read_blocks(
        columns: impl Iterator<Item = Result<Vec<ArrayIter<'_>>>>,
    ) -> BTreeMap<u32, Block>;

    fn read_txs(columns: impl Iterator<Item = Result<Vec<ArrayIter<'_>>>>) -> Vec<Transaction>;

    fn read_logs(columns: impl Iterator<Item = Result<Vec<ArrayIter<'_>>>>) -> Vec<Log>;

    fn block_fields() -> Vec<Field>;

    fn tx_fields() -> Vec<Field>;

    fn log_fields() -> Vec<Field>;
}
