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
mod util;

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

        let (transactions, blocks) =
            if query.mini_query.transactions.is_empty() && transactions.is_empty() {
                (BTreeMap::new(), blocks)
            } else {
                query
                    .clone()
                    .query_transactions(transactions, blocks)
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
        transactions: BTreeSet<(u32, u32)>,
        blocks: BTreeSet<u32>,
    ) -> Result<(BTreeMap<(u32, u32), ResponseTransaction>, BTreeSet<u32>)> {
        let pruned_tx_queries_per_rg: Vec<_> = rayon_async::spawn({
            let query = self.clone();
            move || {
                query
                    .metadata
                    .tx
                    .iter()
                    .map(|rg_meta| {
                        transaction::prune_tx_queries_per_rg(
                            rg_meta,
                            &query.mini_query.transactions,
                            transactions.clone(),
                        )
                    })
                    .collect()
            }
        })
        .await;

        if pruned_tx_queries_per_rg
            .iter()
            .all(|(selections, txs)| selections.is_empty() && txs.is_empty())
        {
            return Ok((BTreeMap::new(), blocks));
        }

        tokio::task::spawn_blocking(move || {
            transaction::query_transactions(self, pruned_tx_queries_per_rg, blocks)
        })
        .await
        .unwrap()
    }

    async fn query_blocks(
        self: Arc<Self>,
        blocks: Option<&BTreeSet<u32>>,
    ) -> Result<BTreeMap<u32, ResponseBlock>> {
        todo!()
    }
}
