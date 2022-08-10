use crate::config::DatafusionConfig;
use crate::types::{QueryLogs, QueryResult, Status};
use crate::{Error, Result};
use eth_archive_core::db::DbHandle;
use std::sync::Arc;

pub struct DataCtx {}

impl DataCtx {
    pub async fn new(db: Arc<DbHandle>, config: DatafusionConfig) -> Result<Self> {
        todo!()
    }

    pub async fn status(&self) -> Result<Status> {
        todo!()
    }

    pub async fn query(&self, query: QueryLogs) -> Result<QueryResult> {
        todo!()
    }
}
