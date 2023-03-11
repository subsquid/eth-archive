use crate::parquet_metadata::ParquetMetadata;
use crate::types::{LogQueryResult, MiniQuery, QueryResult};
use crate::Result;
use eth_archive_core::dir_name::DirName;
use eth_archive_core::types::{ResponseBlock, ResponseTransaction};
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

pub struct ParquetQuery {
    pub data_path: PathBuf,
    pub dir_name: DirName,
    pub metadata: ParquetMetadata,
    pub mini_query: MiniQuery,
}

impl ParquetQuery {
    pub async fn run(self) -> Result<QueryResult> {
        let LogQueryResult {
            logs,
            transactions,
            blocks,
        } = if !self.mini_query.logs.is_empty() {
            self.query_logs().await?
        } else {
            LogQueryResult::default()
        };

        let mut blocks = blocks;

        let transactions = if self.mini_query.transactions.is_empty() && transactions.is_empty() {
            BTreeMap::new()
        } else {
            self.query_transactions(&transactions, &mut blocks).await?
        };

        let blocks = if self.mini_query.include_all_blocks {
            None
        } else {
            Some(&blocks)
        };

        let blocks = self.query_blocks(blocks).await?;

        Ok(QueryResult {
            logs,
            transactions,
            blocks,
        })
    }

    async fn query_logs(&self) -> Result<LogQueryResult> {
        todo!()
    }

    async fn query_transactions(
        &self,
        transactions: &BTreeSet<(u32, u32)>,
        blocks: &mut BTreeSet<u32>,
    ) -> Result<BTreeMap<(u32, u32), ResponseTransaction>> {
        todo!()
    }

    async fn query_blocks(
        &self,
        blocks: Option<&BTreeSet<u32>>,
    ) -> Result<BTreeMap<u32, ResponseBlock>> {
        todo!()
    }
}
