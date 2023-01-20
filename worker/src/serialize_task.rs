use crate::types::{BlockEntry, BlockEntryVec};
use crate::{Error, Result};
use eth_archive_core::rayon_async;
use eth_archive_core::types::{BlockRange, QueryResult};
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
    pub fn new(from_block: u32, size_limit: usize, archive_height: Option<u32>) -> Self {
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

                let new_bytes =
                    rayon_async::spawn(move || process_query_result(bytes, res, is_first)).await;

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

fn process_query_result(mut bytes: Vec<u8>, res: QueryResult, is_first: bool) -> Vec<u8> {
    use std::collections::btree_map::Entry::{Occupied, Vacant};

    let mut data: BTreeMap<u32, BlockEntry> = BTreeMap::new();

    for row in res.data {
        let block_number = row.block.number.unwrap().0;
        let tx_idx = row.transaction.transaction_index.unwrap().0;

        let block_entry = match data.entry(block_number) {
            Vacant(entry) => entry.insert(BlockEntry {
                block: row.block,
                logs: BTreeMap::new(),
                transactions: BTreeMap::new(),
            }),
            Occupied(entry) => entry.into_mut(),
        };

        if let Vacant(entry) = block_entry.transactions.entry(tx_idx) {
            entry.insert(row.transaction);
        }

        if let Some(log) = row.log {
            let log_idx = log.log_index.unwrap().0;
            block_entry.logs.insert(log_idx, log);
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
