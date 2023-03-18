use crate::bloom::Bloom;
use crate::field_selection::FieldSelection;
use arrayvec::ArrayVec;
use eth_archive_core::deserialize::{Address, Bytes32, Index, Sighash};
use eth_archive_core::hash::HashSet;
use eth_archive_core::types::{ResponseBlock, ResponseLog, ResponseTransaction, Transaction};
use serde::{Deserialize, Serialize};
use std::cmp;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone)]
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
    pub address: HashSet<Address>,
    pub topics: ArrayVec<HashSet<Bytes32>, 4>,
}

#[derive(Clone)]
pub struct MiniTransactionSelection {
    pub source: HashSet<Address>,
    pub dest: HashSet<Address>,
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

    pub fn pruned_log_selection(&self, parquet_idx: &Bloom<Address>) -> Vec<MiniLogSelection> {
        self.logs
            .iter()
            .filter_map(|log_selection| {
                let address = &log_selection.address;

                if address.is_empty() {
                    return Some(MiniLogSelection {
                        address: HashSet::default(),
                        topics: log_selection.topics.clone(),
                    });
                }

                let address = address
                    .iter()
                    .filter(|addr| parquet_idx.contains(addr))
                    .cloned()
                    .collect::<HashSet<_>>();

                if !address.is_empty() {
                    Some(MiniLogSelection {
                        address,
                        topics: log_selection.topics.clone(),
                    })
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
    }

    pub fn pruned_tx_selection(
        &self,
        parquet_idx: &Bloom<Address>,
    ) -> Vec<MiniTransactionSelection> {
        self.transactions
            .iter()
            .filter_map(|tx_selection| {
                let source = &tx_selection.source;
                let dest = &tx_selection.dest;

                if source.is_empty() && dest.is_empty() {
                    return Some(MiniTransactionSelection {
                        source: HashSet::default(),
                        dest: HashSet::default(),
                        sighash: tx_selection.sighash.clone(),
                        status: tx_selection.status,
                    });
                }

                let source = source
                    .iter()
                    .filter(|addr| parquet_idx.contains(addr))
                    .cloned()
                    .collect::<HashSet<_>>();

                let dest = dest
                    .iter()
                    .filter(|addr| parquet_idx.contains(addr))
                    .cloned()
                    .collect::<HashSet<_>>();

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
        self.address.is_empty() || self.address.contains(filter_addr)
    }

    fn matches_topics(&self, topics: &[Bytes32]) -> bool {
        for (topic, log_topic) in self.topics.iter().zip(topics.iter()) {
            if !topic.is_empty() && !topic.contains(log_topic) {
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
                match_all_addr || selection.matches_dest(dest) && selection.matches_source(source);

            matches_addr && selection.matches_sighash(sighash) && selection.matches_status(&status)
        })
    }

    fn matches_dest(&self, dest: &Option<Address>) -> bool {
        let tx_addr = match dest.as_ref() {
            Some(addr) => addr,
            None => return false,
        };

        self.dest.contains(tx_addr)
    }

    fn matches_source(&self, source: &Option<Address>) -> bool {
        let tx_addr = match source.as_ref() {
            Some(addr) => addr,
            None => return false,
        };

        self.source.contains(tx_addr)
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
    logs: Vec<LogSelection>,
    #[serde(default)]
    transactions: Vec<TransactionSelection>,
    #[serde(default)]
    include_all_blocks: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct LogSelection {
    #[serde(default)]
    address: Vec<Address>,
    topics: ArrayVec<Vec<Bytes32>, 4>,
    field_selection: FieldSelection,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct TransactionSelection {
    #[serde(rename = "from")]
    #[serde(default)]
    source: Vec<Address>,
    #[serde(rename = "to", alias = "address")]
    #[serde(default)]
    dest: Vec<Address>,
    #[serde(default)]
    sighash: Vec<Sighash>,
    status: Option<u32>,
    field_selection: FieldSelection,
}

impl Query {
    pub fn optimize(&self, archive_height: u32) -> MiniQuery {
        let to_block = match self.to_block {
            Some(to_block) => cmp::min(archive_height, to_block),
            None => archive_height,
        };

        MiniQuery {
            from_block: self.from_block,
            to_block,
            logs: self.log_selection(),
            transactions: self.tx_selection(),
            field_selection: self.field_selection(),
            include_all_blocks: self.include_all_blocks,
        }
    }

    fn field_selection(&self) -> FieldSelection {
        self.logs
            .iter()
            .map(|log| log.field_selection)
            .chain(self.transactions.iter().map(|tx| tx.field_selection))
            .fold(Default::default(), |a, b| a | b)
    }

    fn log_selection(&self) -> Vec<MiniLogSelection> {
        self.logs
            .iter()
            .map(|log| MiniLogSelection {
                address: log.address.iter().cloned().collect(),
                topics: log
                    .topics
                    .iter()
                    .map(|topic| topic.iter().cloned().collect())
                    .collect(),
            })
            .collect()
    }

    fn tx_selection(&self) -> Vec<MiniTransactionSelection> {
        self.transactions
            .iter()
            .map(|transaction| MiniTransactionSelection {
                source: transaction.source.iter().cloned().collect(),
                dest: transaction.dest.iter().cloned().collect(),
                sighash: transaction.sighash.clone(),
                status: transaction.status,
            })
            .collect()
    }
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
