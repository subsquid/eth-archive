use cuckoofilter::{CuckooFilter, ExportedCuckooFilter};
use solana_bloom::bloom::Bloom;
use std::path::Path;

pub struct DbHandle {
    inner: rocksdb::OptimisticTransactionDB,
    consistent_range: Option<(u32, u32)>,
}

impl DbHandle {
    pub async fn new<P: AsRef<Path>>(path: P) -> Result<DbHandle> {
        let mut opts = rocksdb::Options::default();

        let (inner, consistent_range) = tokio::spawn_blocking(move || {
            let inner =
                rocksdb::OptimisticTransactionDB::open_cf(&opts, path, cf_name::ALL_CF_NAMES)
                    .map_err(Error::OpenDb)?;

            let consistent_range = Self::get_consistent_range(&inner).map_err(Error::GetMaxBlock)?;

            Ok((inner, consistent_range))
        });

        Ok(Self { inner, consistent_range })
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

    pub fn consistent_range(&self) -> Option<(u32, u32)> {
        self.consistent_range
    }

    fn get_consistent_range(inner: &rocksdb::OptimisticTransactionDB) -> Result<Option<(u32, u32)>> {
        todo!()
    }
}

pub const LOG_ADDR_FILTER: &str = "LOG_ADDR_FILTER";
pub const TX_ADDR_FILTER: &str = "TX_ADDR_FILTER";

/// Column Family Names
mod cf_name {
    pub const BLOCK: &str = "BLOCK";
    pub const TX: &str = "TX";
    pub const LOG: &str = "LOG";
    pub const ADDR_LOG: &str = "ADDR_LOG";
    pub const ADDR_TX: &str = "ADDR_TX";
    pub const PARQUET_IDX: &str = "PARQUET_FOLDERS";

    pub const ALL_CF_NAMES: [&str; 6] = [
        BLOCK,
        TX,
        LOG,
        ADDR_LOG,
        ADDR_TX,
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
