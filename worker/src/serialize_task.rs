use crate::field_selection::FieldSelection;
use crate::types::QueryResult;
use crate::{Error, Result};
use eth_archive_core::types::{BlockRange, ResponseBlock, ResponseLog, ResponseTransaction};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io::Write;
use std::mem;
use std::time::Instant;
use tokio::sync::mpsc;

type Sender = mpsc::Sender<(QueryResult, BlockRange)>;

pub struct SerializeTask {
    tx: Sender,
    join_handle: tokio::task::JoinHandle<Vec<u8>>,
}

impl SerializeTask {
    pub fn new(
        from_block: u32,
        size_limit: usize,
        time_limit: u128,
        archive_height: Option<u32>,
        field_selection: FieldSelection,
    ) -> Self {
        let (tx, mut rx): (Sender, _) = mpsc::channel(1);

        // convert size limit to bytes from megabytes
        let size_limit = size_limit * 1_000_000;

        let join_handle = tokio::spawn(async move {
            let query_start = Instant::now();

            let mut bytes = br#"{"data":["#.to_vec();

            let mut is_first = true;

            let mut next_block = from_block;

            while let Some((res, range)) = rx.recv().await {
                next_block = range.to;

                if query_start.elapsed().as_millis() >= time_limit {
                    break;
                }

                if res.is_empty() {
                    continue;
                }

                bytes = tokio::task::spawn_blocking(move || {
                    process_query_result(bytes, res, is_first, field_selection)
                })
                .await
                .unwrap();

                is_first = false;

                if bytes.len() >= size_limit {
                    break;
                }
            }

            let archive_height = match archive_height {
                Some(archive_height) => archive_height.to_string(),
                None => "null".to_owned(),
            };

            write!(
                &mut bytes,
                r#"],"archiveHeight":{},"nextBlock":{},"totalTime":{}}}"#,
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

        let res = self.join_handle.await.map_err(Error::TaskJoinError);
        res
    }

    pub async fn send(&self, msg: (QueryResult, BlockRange)) -> bool {
        self.tx.send(msg).await.is_ok()
    }

    pub fn is_closed(&self) -> bool {
        self.tx.is_closed()
    }
}

fn process_query_result(
    mut bytes: Vec<u8>,
    res: QueryResult,
    is_first: bool,
    field_selection: FieldSelection,
) -> Vec<u8> {
    let mut data: BTreeMap<u32, BlockEntry> = BTreeMap::new();

    for (block_num, block) in res.blocks.into_iter() {
        let block = if field_selection.block == Default::default() {
            None
        } else {
            Some(field_selection.block.prune_opt(block))
        };

        data.insert(
            block_num,
            BlockEntry {
                block,
                transactions: BTreeMap::new(),
                logs: BTreeMap::new(),
            },
        );
    }

    for ((block_num, transaction_index), transaction) in res.transactions.into_iter() {
        if field_selection.transaction == Default::default() {
            continue;
        }

        let transaction = field_selection.transaction.prune_opt(transaction);

        let entry = data.get_mut(&block_num).unwrap();
        entry.transactions.insert(transaction_index, transaction);
    }

    for ((block_num, log_index), log) in res.logs.into_iter() {
        if field_selection.log == Default::default() {
            continue;
        }

        let log = field_selection.log.prune_opt(log);

        let entry = data.get_mut(&block_num).unwrap();
        entry.logs.insert(log_index, log);
    }

    let data = data
        .into_values()
        .map(BlockEntryVec::from)
        .collect::<Vec<BlockEntryVec>>();

    if !is_first {
        bytes.push(b',');
    }

    simd_json::to_writer(&mut bytes, &data).unwrap();

    bytes
}

pub struct BlockEntry {
    pub block: Option<ResponseBlock>,
    pub transactions: BTreeMap<u32, ResponseTransaction>,
    pub logs: BTreeMap<u32, ResponseLog>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BlockEntryVec {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block: Option<ResponseBlock>,
    pub transactions: Vec<ResponseTransaction>,
    pub logs: Vec<ResponseLog>,
}

impl From<BlockEntry> for BlockEntryVec {
    fn from(entry: BlockEntry) -> Self {
        Self {
            block: entry.block,
            transactions: entry.transactions.into_values().collect(),
            logs: entry.logs.into_values().collect(),
        }
    }
}
