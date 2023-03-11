use crate::parquet_metadata::ParquetMetadata;
use crate::types::{LogQueryResult, MiniQuery, QueryResult};
use crate::Result;
use eth_archive_core::dir_name::DirName;
use eth_archive_core::rayon_async;
use eth_archive_core::types::{ResponseBlock, ResponseTransaction};
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;

mod block;
mod log;
mod transaction;

pub struct ParquetQuery {
    pub data_path: PathBuf,
    pub dir_name: DirName,
    pub metadata: ParquetMetadata,
    pub mini_query: MiniQuery,
}

impl ParquetQuery {
    pub async fn run(self) -> Result<QueryResult> {
        let query = Arc::new(self);

        let LogQueryResult {
            logs,
            transactions,
            blocks,
        } = if !query.mini_query.logs.is_empty() {
            query.clone().query_logs().await?
        } else {
            LogQueryResult::default()
        };

        let mut blocks = blocks;

        let transactions = if query.mini_query.transactions.is_empty() && transactions.is_empty() {
            BTreeMap::new()
        } else {
            query
                .clone()
                .query_transactions(&transactions, &mut blocks)
                .await?
        };

        let blocks = if query.mini_query.include_all_blocks {
            None
        } else {
            Some(&blocks)
        };

        let blocks = query.query_blocks(blocks).await?;

        Ok(QueryResult {
            logs,
            transactions,
            blocks,
        })
    }

    async fn query_logs(self: Arc<Self>) -> Result<LogQueryResult> {
        let pruned_queries_per_rg: Vec<_> = rayon_async::spawn({
            let query = self.clone();
            move || {
                query
                    .metadata
                    .log
                    .iter()
                    .map(|rg_meta| log::prune_log_queries_per_rg(rg_meta, &query.mini_query.logs))
                    .collect()
            }
        })
        .await;

        if pruned_queries_per_rg.iter().all(Vec::is_empty) {
            return Ok(LogQueryResult::default());
        }

        tokio::task::spawn_blocking(move || log::query_logs(self, pruned_queries_per_rg))
            .await
            .unwrap()
    }

    async fn query_transactions(
        self: Arc<Self>,
        transactions: &BTreeSet<(u32, u32)>,
        blocks: &mut BTreeSet<u32>,
    ) -> Result<BTreeMap<(u32, u32), ResponseTransaction>> {
        todo!()
    }

    async fn query_blocks(
        self: Arc<Self>,
        blocks: Option<&BTreeSet<u32>>,
    ) -> Result<BTreeMap<u32, ResponseBlock>> {
        todo!()
    }
}
