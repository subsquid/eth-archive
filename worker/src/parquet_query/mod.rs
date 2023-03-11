use crate::parquet_metadata::{ParquetMetadata, LogRowGroupMetadata, hash};
use crate::types::{LogQueryResult, MiniQuery, QueryResult, MiniLogSelection};
use crate::{Error, Result};
use eth_archive_core::dir_name::DirName;
use eth_archive_core::types::{ResponseBlock, ResponseTransaction};
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use eth_archive_core::rayon_async;
use std::sync::Arc;
use xorf::Filter;
use arrow2::io::parquet;
use std::{io, fs};
use eth_archive_ingester::schema::log_schema;

mod log;

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
            query.clone().query_transactions(&transactions, &mut blocks).await?
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
        let pruned_queries_per_rg = self.clone().prune_log_queries_per_rg().await;
        
        if pruned_queries_per_rg.iter().all(Vec::is_empty) {
            return Ok(LogQueryResult::default());
        }
        
        tokio::task::spawn_blocking(move || {
            
        }).await.unwrap()
    }
    
    async fn prune_log_queries_per_rg(self: Arc<Self>) -> Vec<Vec<MiniLogSelection>> {
        rayon_async::spawn(move || {
            self.metadata.log.iter().map(|rg_meta| {
                log::prune_log_queries_per_rg(rg_meta, &self.mini_query.logs)
            }).collect()
        }).await
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


