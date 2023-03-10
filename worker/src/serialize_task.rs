use crate::field_selection::FieldSelection;
use crate::{Error, Result};
use crate::types::QueryResult;
use eth_archive_core::rayon_async;
use eth_archive_core::types::{
    BlockRange, ResponseBlock, ResponseLog, ResponseTransaction,
};
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

                if res.data.is_empty() {
                    continue;
                }

                let new_bytes = rayon_async::spawn(move || {
                    process_query_result(bytes, res, is_first, field_selection)
                })
                .await;

                bytes = new_bytes;

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

        self.join_handle.await.map_err(Error::TaskJoinError)
    }

    pub fn send(&self, msg: (QueryResult, BlockRange)) -> bool {
        self.tx.blocking_send(msg).is_ok()
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
    use std::collections::btree_map::Entry::{Occupied, Vacant};

    let mut data: BTreeMap<u32, BlockEntry> = BTreeMap::new();

    for row in res.data {
        let block_number = row.block.number.unwrap().0;

        let block_entry = match data.entry(block_number) {
            Vacant(entry) => entry.insert(BlockEntry {
                block: Some(row.block),
                logs: BTreeMap::new(),
                transactions: BTreeMap::new(),
            }),
            Occupied(entry) => entry.into_mut(),
        };

        if let Some(tx) = row.transaction {
            let tx_idx = tx.transaction_index.unwrap().0;

            if let Vacant(entry) = block_entry.transactions.entry(tx_idx) {
                entry.insert(tx);
            }
        }

        if let Some(log) = row.log {
            let log_idx = log.log_index.unwrap().0;
            block_entry.logs.insert(log_idx, log);
        }
    }

    for (_, entry) in data.iter_mut() {
        if field_selection.block == Default::default() {
            entry.block = None;
        } else {
            entry.block = mem::take(&mut entry.block).map(|b| field_selection.block.prune_opt(b));
        }

        if field_selection.transaction == Default::default() {
            mem::take(&mut entry.transactions);
        } else {
            entry.transactions = mem::take(&mut entry.transactions)
                .into_iter()
                .map(|(i, t)| (i, field_selection.transaction.prune_opt(t)))
                .collect();
        }

        if field_selection.log == Default::default() {
            mem::take(&mut entry.logs);
        } else {
            entry.logs = mem::take(&mut entry.logs)
                .into_iter()
                .map(|(i, l)| (i, field_selection.log.prune_opt(l)))
                .collect();
        }
    }

    let data = data
        .into_values()
        .map(BlockEntryVec::from)
        .collect::<Vec<BlockEntryVec>>();

    if !is_first {
        bytes.push(b',');
    }

    serde_json::to_writer(&mut bytes, &data).unwrap();

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
