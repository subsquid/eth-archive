use crate::field_selection::FieldSelection;
use arrayvec::ArrayVec;
use eth_archive_core::deserialize::{Address, Bytes32, Index, Sighash};
use eth_archive_core::hash::hash;
use eth_archive_core::types::{ResponseBlock, ResponseLog, ResponseTransaction, Transaction};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use xorf::{BinaryFuse8, Filter};

pub struct MiniQuery {
    pub from_block: u32,
    pub to_block: u32,
    pub logs: Vec<MiniLogSelection>,
    pub transactions: Vec<MiniTransactionSelection>,
    pub field_selection: FieldSelection,
    pub include_all_blocks: bool,
}

#[derive(Clone)]
pub struct MiniLogSelection {
    pub address: Vec<(Address, u64)>,
    pub topics: ArrayVec<Vec<Bytes32>, 4>,
    pub topic0_hash: Option<Vec<u64>>,
}

#[derive(Clone)]
pub struct MiniTransactionSelection {
    pub source: Vec<(Address, u64)>,
    pub dest: Vec<(Address, u64)>,
    pub sighash: Vec<Sighash>,
    pub status: Option<u32>,
}

#[derive(Default)]
pub struct LogQueryResult {
    pub logs: BTreeMap<(u32, u32), ResponseLog>,
    pub transactions: BTreeSet<(u32, u32)>,
    pub blocks: BTreeSet<u32>,
}

impl MiniQuery {
    pub fn matches_log(&self, address: &Address, topics: &[Bytes32]) -> bool {
        MiniLogSelection::matches_log_impl(&self.logs, address, topics)
    }

    #[allow(clippy::match_like_matches_macro)]
    pub fn matches_tx(&self, tx: &Transaction) -> bool {
        MiniTransactionSelection::matches_tx_impl(
            &self.transactions,
            &tx.source,
            &tx.dest,
            &tx.input.get(..4).map(Sighash::new),
            tx.status,
        )
    }
}

impl MiniLogSelection {
    pub fn matches_log_impl(
        filters: &[MiniLogSelection],
        address: &Address,
        topics: &[Bytes32],
    ) -> bool {
        filters
            .iter()
            .any(|selection| selection.matches_addr(address) && selection.matches_topics(topics))
    }

    fn matches_addr(&self, filter_addr: &Address) -> bool {
        self.address.is_empty() || self.address.iter().any(|addr| &addr.0 == filter_addr)
    }

    fn matches_topics(&self, topics: &[Bytes32]) -> bool {
        for (topic, log_topic) in self.topics.iter().zip(topics.iter()) {
            if !topic.is_empty() && !topic.iter().any(|topic| log_topic == topic) {
                return false;
            }
        }

        true
    }
}

impl MiniTransactionSelection {
    pub fn matches_tx_impl(
        filters: &[MiniTransactionSelection],
        source: &Option<Address>,
        dest: &Option<Address>,
        sighash: &Option<Sighash>,
        status: Option<Index>,
    ) -> bool {
        filters.iter().any(|selection| {
            let match_all_addr = selection.source.is_empty() && selection.dest.is_empty();
            let matches_addr =
                match_all_addr || selection.matches_dest(dest) || selection.matches_source(source);

            matches_addr && selection.matches_sighash(sighash) && selection.matches_status(&status)
        })
    }

    fn matches_dest(&self, dest: &Option<Address>) -> bool {
        let tx_addr = match dest.as_ref() {
            Some(addr) => addr,
            None => return false,
        };

        self.dest.iter().any(|addr| &addr.0 == tx_addr)
    }

    fn matches_source(&self, source: &Option<Address>) -> bool {
        let tx_addr = match source.as_ref() {
            Some(addr) => addr,
            None => return false,
        };

        self.source.iter().any(|addr| &addr.0 == tx_addr)
    }

    fn matches_sighash(&self, sighash: &Option<Sighash>) -> bool {
        let sighash = match sighash {
            Some(sig) => sig,
            None => return false,
        };

        self.sighash.is_empty() || self.sighash.iter().any(|sig| sig == sighash)
    }

    fn matches_status(&self, tx_status: &Option<Index>) -> bool {
        match (self.status, tx_status) {
            (Some(status), Some(tx_status)) => status == tx_status.0,
            _ => true,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Query {
    pub from_block: u32,
    pub to_block: Option<u32>,
    #[serde(default)]
    pub logs: Vec<LogSelection>,
    #[serde(default)]
    pub transactions: Vec<TransactionSelection>,
    #[serde(default)]
    pub include_all_blocks: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct LogSelection {
    #[serde(default)]
    pub address: Vec<Address>,
    pub topics: ArrayVec<Vec<Bytes32>, 4>,
    pub field_selection: FieldSelection,
}

impl LogSelection {
    fn topic0_hash(&self) -> Option<Vec<u64>> {
        match self.topics.get(0) {
            Some(topic0) if !topic0.is_empty() => {
                Some(topic0.iter().map(|t| hash(t.as_slice())).collect())
            }
            _ => None,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct TransactionSelection {
    #[serde(rename = "from")]
    #[serde(default)]
    pub source: Vec<Address>,
    #[serde(rename = "to", alias = "address")]
    #[serde(default)]
    pub dest: Vec<Address>,
    #[serde(default)]
    pub sighash: Vec<Sighash>,
    pub status: Option<u32>,
    pub field_selection: FieldSelection,
}

#[derive(Default)]
pub struct QueryResult {
    pub logs: BTreeMap<(u32, u32), ResponseLog>,
    pub transactions: BTreeMap<(u32, u32), ResponseTransaction>,
    pub blocks: BTreeMap<u32, ResponseBlock>,
}

impl QueryResult {
    pub fn is_empty(&self) -> bool {
        self.logs.is_empty() && self.transactions.is_empty() && self.blocks.is_empty()
    }
}

impl Query {
    pub fn field_selection(&self) -> FieldSelection {
        self.logs
            .iter()
            .map(|log| log.field_selection)
            .chain(self.transactions.iter().map(|tx| tx.field_selection))
            .fold(Default::default(), |a, b| a | b)
    }

    pub fn pruned_log_selection(&self, parquet_idx: &BinaryFuse8) -> Vec<MiniLogSelection> {
        self.logs
            .iter()
            .filter_map(|log_selection| {
                let address = &log_selection.address;

                if address.is_empty() {
                    return Some(MiniLogSelection {
                        address: Vec::new(),
                        topics: log_selection.topics.clone(),
                        topic0_hash: log_selection.topic0_hash(),
                    });
                }

                let address = address
                    .iter()
                    .filter_map(|addr| {
                        let h = hash(addr.as_slice());
                        if parquet_idx.contains(&h) {
                            Some((addr.clone(), h))
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>();

                if !address.is_empty() {
                    Some(MiniLogSelection {
                        address,
                        topics: log_selection.topics.clone(),
                        topic0_hash: log_selection.topic0_hash(),
                    })
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
    }

    pub fn pruned_tx_selection(&self, parquet_idx: &BinaryFuse8) -> Vec<MiniTransactionSelection> {
        self.transactions
            .iter()
            .filter_map(|tx_selection| {
                let source = &tx_selection.source;
                let dest = &tx_selection.dest;

                if source.is_empty() && dest.is_empty() {
                    return Some(MiniTransactionSelection {
                        source: Vec::new(),
                        dest: Vec::new(),
                        sighash: tx_selection.sighash.clone(),
                        status: tx_selection.status,
                    });
                }

                let source = source
                    .iter()
                    .filter_map(|addr| {
                        let h = hash(addr.as_slice());
                        if parquet_idx.contains(&h) {
                            Some((addr.clone(), h))
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>();

                let dest = dest
                    .iter()
                    .filter_map(|addr| {
                        let h = hash(addr.as_slice());
                        if parquet_idx.contains(&h) {
                            Some((addr.clone(), h))
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>();

                if source.is_empty() && dest.is_empty() {
                    None
                } else {
                    Some(MiniTransactionSelection {
                        source,
                        dest,
                        sighash: tx_selection.sighash.clone(),
                        status: tx_selection.status,
                    })
                }
            })
            .collect::<Vec<_>>()
    }

    pub fn log_selection(&self) -> Vec<MiniLogSelection> {
        self.logs
            .iter()
            .map(|log| MiniLogSelection {
                address: log
                    .address
                    .iter()
                    .map(|addr| (addr.clone(), hash(addr.as_slice())))
                    .collect(),
                topics: log.topics.clone(),
                topic0_hash: log.topic0_hash(),
            })
            .collect()
    }

    pub fn tx_selection(&self) -> Vec<MiniTransactionSelection> {
        self.transactions
            .iter()
            .map(|transaction| MiniTransactionSelection {
                source: transaction
                    .source
                    .iter()
                    .map(|addr| (addr.clone(), hash(addr.as_slice())))
                    .collect(),
                dest: transaction
                    .dest
                    .iter()
                    .map(|addr| (addr.clone(), hash(addr.as_slice())))
                    .collect(),
                sighash: transaction.sighash.clone(),
                status: transaction.status,
            })
            .collect()
    }
}
