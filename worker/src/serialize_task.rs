use crate::types::BlockEntry;
use crate::{Error, Result};
use eth_archive_core::rayon_async;
use eth_archive_core::types::{BlockRange, QueryMetrics, QueryResult};
use std::collections::HashMap;
use std::mem;
use std::time::Instant;
use tokio::sync::mpsc;

pub struct SerializeTask {
    tx: mpsc::Sender<(QueryResult, BlockRange)>,
    join_handle: tokio::task::JoinHandle<Vec<u8>>,
}

impl SerializeTask {
    pub fn new(from_block: u32, size_limit: usize, archive_height: Option<u32>) -> Self {
        let (tx, mut rx) = mpsc::channel(1);

        let join_handle = tokio::spawn(async move {
            let query_start = Instant::now();

            let mut bytes = br#"{"data":["#.to_vec();

            let mut is_first = true;

            let mut next_block = from_block;

            let mut metrics = QueryMetrics::default();

            while let Some((res, range)) = rx.recv().await {
                next_block = range.to;
                metrics += res.metrics;

                if res.data.is_empty() {
                    continue;
                }

                let (new_bytes, new_metrics) =
                    rayon_async::spawn(move || process_query_result(bytes, res, is_first)).await;

                bytes = new_bytes;
                metrics += new_metrics;

                is_first = false;

                if bytes.len() >= size_limit {
                    break;
                }
            }

            let metrics = serde_json::to_string(&metrics).unwrap();

            let archive_height = match archive_height {
                Some(archive_height) => archive_height.to_string(),
                None => "null".to_owned(),
            };

            write!(
                &mut bytes,
                r#"],"metrics":{},"archive_height":{},"nextBlock":{},"totalTime":{}}}"#,
                metrics,
                archive_height,
                next_block,
                query_start.elapsed().as_millis(),
            )
            .unwrap();

            bytes
        });

        Self { tx, join_handle }
    }

    pub async fn join(self) -> Result<Vec<u8>> {
        mem::drop(self.tx);

        self.join_handle.await.map_err(Error::TaskJoinError)
    }

    pub async fn send(&self, msg: (QueryResult, BlockRange)) -> bool {
        self.tx.send(msg).await.is_ok()
    }
}

fn process_query_result(
    mut bytes: Vec<u8>,
    res: QueryResult,
    is_first: bool,
) -> (Vec<u8>, QueryMetrics) {
    let mut data = Vec::new();

    let mut block_idxs = HashMap::new();
    let mut tx_idxs = HashMap::new();

    for row in res.data {
        let block_number = row.block.number.unwrap().0;
        let tx_hash = row.transaction.hash.clone().unwrap().0;

        let block_idx = match block_idxs.get(&block_number) {
            Some(block_idx) => *block_idx,
            None => {
                let block_idx = data.len();

                data.push(BlockEntry {
                    block: row.block,
                    logs: Vec::new(),
                    transactions: Vec::new(),
                });

                block_idxs.insert(block_number, block_idx);

                block_idx
            }
        };

        if let std::collections::hash_map::Entry::Vacant(e) = tx_idxs.entry(tx_hash) {
            e.insert(data[block_idx].transactions.len());
            data[block_idx].transactions.push(row.transaction);
        }

        if let Some(log) = row.log {
            data[block_idx].logs.push(log);
        }
    }

    if !is_first {
        bytes.push(b',');
    }

    let start = Instant::now();

    serde_json::to_writer(&mut bytes, &data).unwrap();

    let elapsed = start.elapsed().as_millis();

    let mut metrics = QueryMetrics::default();
    metrics.serialize_result += elapsed;
    metrics.total += elapsed;

    (bytes, metrics)
}
