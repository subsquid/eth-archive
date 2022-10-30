use cuckoofilter::{CuckooFilter, ExportedCuckooFilter};
use solana_bloom::bloom::Bloom;
use std::path::Path;

pub struct DbHandle {
    inner: rocksdb::OptimisticTransactionDB,
    max_block: Option<u32>,
}

impl DbHandle {
    pub async fn new<P: AsRef<Path>>(path: P) -> Result<DbHandle> {
        let mut opts = rocksdb::Options::default();

        let (inner, max_block) = tokio::spawn_blocking(move || {
            let inner =
                rocksdb::OptimisticTransactionDB::open_cf(&opts, path, cf_name::ALL_CF_NAMES)
                    .map_err(Error::OpenDb)?;

            let max_block = Self::get_max_block(&inner).map_err(Error::GetMaxBlock)?;

            Ok((inner, max_block))
        });

        Ok(Self { inner, max_block })
    }

    pub async fn get_next_parquet_folder(
        self: Arc<Self>,
        from_block: u32,
    ) -> Result<Option<DirName>> {
        todo!()
    }

    pub async fn query(self: Arc<Self>, query: MiniQuery) -> Result<QueryResult> {
        todo!()
    }

    /// WARNING: only call from blocking context
    pub fn insert_parquet_folder_if_not_exists(&self, dir_name: DirName) -> Result<()> {
        todo!()
    }

    /// WARNING: only call from blocking context
    pub fn insert_batches(&self, batches: &[(Vec<Vec<Block>>, Vec<Vec<Log>>)]) -> Result<()> {
        todo!()
    }

    /// WARNING: only call from blocking context
    pub fn delete_tail(&self) -> Result<()> {
        todo!()
    }

    pub fn max_block(&self) -> Option<u32> {
        self.max_block
    }

    fn get_max_block(inner: &rocksdb::OptimisticTransactionDB) -> Result<Option<u32>> {
        todo!()
    }
}

/// Column Family Names
mod cf_name {
    pub const BLOCK: &str = "BLOCK";
    pub const TX: &str = "TX";
    pub const LOG: &str = "LOG";
    pub const ADDR_LOG: &str = "ADDR_LOG";
    pub const ADDR_TX: &str = "ADDR_TX";
    pub const LOG_ADDR_FILTER: &str = "LOG_ADDR_FILTER";
    pub const TX_ADDR_FILTER: &str = "TX_ADDR_FILTER";
    pub const PARQUET_IDX: &str = "PARQUET_FOLDERS";

    pub const ALL_CF_NAMES: [&str; 8] = [
        BLOCK,
        TX,
        LOG,
        ADDR_LOG,
        ADDR_TX,
        LOG_ADDR_FILTER,
        TX_ADDR_FILTER,
        PARQUET_IDX,
    ];
}

#[derive(Serialize, Deserialize)]
pub struct ParquetIdx {
    pub log_addr_filter: Bloom,
    pub tx_addr_filter: Bloom,
}

#[derive(Serialize, Deserialize)]
struct ExportedCuckoo {
    values: Vec<u8>,
    length: usize,
}

impl From<&CuckooFilter> for ExportedCuckoo {
    fn from(filter: &CuckooFilter) -> Self {
        let filter = filter.export();

        Self {
            values: filter.values,
            length: filter.length,
        }
    }
}

impl From<ExportedCuckoo> for CuckooFilter {
    fn from(filter: ExportedCuckoo) -> Self {
        Self::from(ExportedCuckooFilter {
            values: filter.values,
            length: filter.length,
        })
    }
}
