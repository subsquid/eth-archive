use eth_archive_core::types::{
    Block, Log, ResponseBlock, ResponseLog, ResponseTransaction, Transaction,
};
use polars::prelude::*;
use serde::Deserialize;

macro_rules! append_col {
    ($table_name:expr, $cols:ident, $self:ident, $field:ident) => {
        if let Some(true) = $self.$field {
            let field_name = stringify!($field);
            let col = col(&format!("{}", field_name));
            let col = col.prefix(&format!("{}_", $table_name));
            $cols.push(col);
        }
    };
}

macro_rules! prune_col {
    ($src:ident, $self:ident, $field:ident) => {
        $self
            .$field
            .map(|cond| if cond { Some($src.$field) } else { None })
            .flatten()
    };
}

#[derive(Deserialize, Debug, Clone, Copy, Default)]
#[serde(rename_all = "camelCase")]
pub struct FieldSelection {
    pub block: Option<BlockFieldSelection>,
    pub transaction: Option<TransactionFieldSelection>,
    pub log: Option<LogFieldSelection>,
}

impl FieldSelection {
    pub fn merge(left: Option<Self>, right: Option<Self>) -> Option<Self> {
        let left = match left {
            Some(left) => left,
            None => return right,
        };

        let right = match right {
            Some(right) => right,
            None => return Some(left),
        };

        Some(Self {
            block: BlockFieldSelection::merge(left.block, right.block),
            transaction: TransactionFieldSelection::merge(left.transaction, right.transaction),
            log: LogFieldSelection::merge(left.log, right.log),
        })
    }
}

#[derive(Deserialize, Debug, Clone, Copy, Default)]
#[serde(rename_all = "camelCase")]
pub struct BlockFieldSelection {
    pub number: Option<bool>,
    pub hash: Option<bool>,
    pub parent_hash: Option<bool>,
    pub nonce: Option<bool>,
    pub sha3_uncles: Option<bool>,
    pub logs_bloom: Option<bool>,
    pub transactions_root: Option<bool>,
    pub state_root: Option<bool>,
    pub receipts_root: Option<bool>,
    pub miner: Option<bool>,
    pub difficulty: Option<bool>,
    pub total_difficulty: Option<bool>,
    pub extra_data: Option<bool>,
    pub size: Option<bool>,
    pub gas_limit: Option<bool>,
    pub gas_used: Option<bool>,
    pub timestamp: Option<bool>,
}

impl BlockFieldSelection {
    pub fn to_cols(self) -> Vec<Expr> {
        let mut cols = Vec::new();

        let table_name = "block";
        append_col!(table_name, cols, self, number);
        append_col!(table_name, cols, self, hash);
        append_col!(table_name, cols, self, parent_hash);
        append_col!(table_name, cols, self, nonce);
        append_col!(table_name, cols, self, sha3_uncles);
        append_col!(table_name, cols, self, logs_bloom);
        append_col!(table_name, cols, self, transactions_root);
        append_col!(table_name, cols, self, state_root);
        append_col!(table_name, cols, self, receipts_root);
        append_col!(table_name, cols, self, miner);
        append_col!(table_name, cols, self, difficulty);
        append_col!(table_name, cols, self, total_difficulty);
        append_col!(table_name, cols, self, extra_data);
        append_col!(table_name, cols, self, size);
        append_col!(table_name, cols, self, gas_limit);
        append_col!(table_name, cols, self, gas_used);
        append_col!(table_name, cols, self, timestamp);

        cols
    }

    pub fn prune(&self, block: Block) -> ResponseBlock {
        ResponseBlock {
            number: prune_col!(block, self, number),
            hash: prune_col!(block, self, hash),
            parent_hash: prune_col!(block, self, parent_hash),
            nonce: prune_col!(block, self, nonce),
            sha3_uncles: prune_col!(block, self, sha3_uncles),
            logs_bloom: prune_col!(block, self, logs_bloom),
            transactions_root: prune_col!(block, self, transactions_root),
            state_root: prune_col!(block, self, state_root),
            receipts_root: prune_col!(block, self, receipts_root),
            miner: prune_col!(block, self, miner),
            difficulty: prune_col!(block, self, difficulty),
            total_difficulty: prune_col!(block, self, total_difficulty),
            extra_data: prune_col!(block, self, extra_data),
            size: prune_col!(block, self, size),
            gas_limit: prune_col!(block, self, gas_limit),
            gas_used: prune_col!(block, self, gas_used),
            timestamp: prune_col!(block, self, timestamp),
        }
    }

    pub fn merge(left: Option<Self>, right: Option<Self>) -> Option<Self> {
        let left = match left {
            Some(left) => left,
            None => return right,
        };

        let right = match right {
            Some(right) => right,
            None => return Some(left),
        };

        Some(Self {
            number: merge_opt(left.number, right.number),
            hash: merge_opt(left.hash, right.hash),
            parent_hash: merge_opt(left.parent_hash, right.parent_hash),
            nonce: merge_opt(left.nonce, right.nonce),
            sha3_uncles: merge_opt(left.sha3_uncles, right.sha3_uncles),
            logs_bloom: merge_opt(left.logs_bloom, right.logs_bloom),
            transactions_root: merge_opt(left.transactions_root, right.transactions_root),
            state_root: merge_opt(left.state_root, right.state_root),
            receipts_root: merge_opt(left.receipts_root, right.receipts_root),
            miner: merge_opt(left.miner, right.miner),
            difficulty: merge_opt(left.difficulty, right.difficulty),
            total_difficulty: merge_opt(left.total_difficulty, right.total_difficulty),
            extra_data: merge_opt(left.extra_data, right.extra_data),
            size: merge_opt(left.size, right.size),
            gas_limit: merge_opt(left.gas_limit, right.gas_limit),
            gas_used: merge_opt(left.gas_used, right.gas_used),
            timestamp: merge_opt(left.timestamp, right.timestamp),
        })
    }
}

#[derive(Deserialize, Debug, Clone, Copy, Default)]
#[serde(rename_all = "camelCase")]
pub struct TransactionFieldSelection {
    block_hash: Option<bool>,
    pub block_number: Option<bool>,
    #[serde(rename = "from")]
    pub source: Option<bool>,
    pub gas: Option<bool>,
    pub gas_price: Option<bool>,
    pub hash: Option<bool>,
    pub input: Option<bool>,
    pub nonce: Option<bool>,
    #[serde(rename = "to")]
    pub dest: Option<bool>,
    #[serde(rename = "index")]
    pub transaction_index: Option<bool>,
    pub value: Option<bool>,
    pub kind: Option<bool>,
    pub chain_id: Option<bool>,
    pub v: Option<bool>,
    pub r: Option<bool>,
    pub s: Option<bool>,
}

impl TransactionFieldSelection {
    pub fn to_cols(self) -> Vec<Expr> {
        let mut cols = Vec::new();

        let table_name = "tx";
        append_col!(table_name, cols, self, block_hash);
        append_col!(table_name, cols, self, block_number);
        append_col!(table_name, cols, self, source);
        append_col!(table_name, cols, self, gas);
        append_col!(table_name, cols, self, gas_price);
        append_col!(table_name, cols, self, hash);
        append_col!(table_name, cols, self, input);
        append_col!(table_name, cols, self, nonce);
        append_col!(table_name, cols, self, dest);
        append_col!(table_name, cols, self, transaction_index);
        append_col!(table_name, cols, self, value);
        append_col!(table_name, cols, self, kind);
        append_col!(table_name, cols, self, chain_id);
        append_col!(table_name, cols, self, v);
        append_col!(table_name, cols, self, r);
        append_col!(table_name, cols, self, s);

        cols
    }

    pub fn prune(&self, tx: Transaction) -> ResponseTransaction {
        ResponseTransaction {
            block_hash: prune_col!(tx, self, block_hash),
            block_number: prune_col!(tx, self, block_number),
            source: prune_col!(tx, self, source),
            gas: prune_col!(tx, self, gas),
            gas_price: prune_col!(tx, self, gas_price),
            hash: prune_col!(tx, self, hash),
            input: prune_col!(tx, self, input),
            nonce: prune_col!(tx, self, nonce),
            dest: prune_col!(tx, self, dest).flatten(),
            transaction_index: prune_col!(tx, self, transaction_index),
            value: prune_col!(tx, self, value),
            kind: prune_col!(tx, self, kind),
            chain_id: prune_col!(tx, self, chain_id).flatten(),
            v: prune_col!(tx, self, v),
            r: prune_col!(tx, self, r),
            s: prune_col!(tx, self, s),
        }
    }

    pub fn merge(left: Option<Self>, right: Option<Self>) -> Option<Self> {
        let left = match left {
            Some(left) => left,
            None => return right,
        };

        let right = match right {
            Some(right) => right,
            None => return Some(left),
        };

        Some(Self {
            block_hash: merge_opt(left.block_hash, right.block_hash),
            block_number: merge_opt(left.block_number, right.block_number),
            source: merge_opt(left.source, right.source),
            gas: merge_opt(left.gas, right.gas),
            gas_price: merge_opt(left.gas_price, right.gas_price),
            hash: merge_opt(left.hash, right.hash),
            input: merge_opt(left.input, right.input),
            nonce: merge_opt(left.nonce, right.nonce),
            dest: merge_opt(left.dest, right.dest),
            transaction_index: merge_opt(left.transaction_index, right.transaction_index),
            value: merge_opt(left.value, right.value),
            kind: merge_opt(left.kind, right.kind),
            chain_id: merge_opt(left.chain_id, right.chain_id),
            v: merge_opt(left.v, right.v),
            r: merge_opt(left.r, right.r),
            s: merge_opt(left.s, right.s),
        })
    }
}

#[derive(Deserialize, Debug, Clone, Copy, Default)]
#[serde(rename_all = "camelCase")]
pub struct LogFieldSelection {
    pub address: Option<bool>,
    pub block_hash: Option<bool>,
    pub block_number: Option<bool>,
    pub data: Option<bool>,
    #[serde(rename = "index")]
    pub log_index: Option<bool>,
    pub removed: Option<bool>,
    pub topics: Option<bool>,
    pub transaction_hash: Option<bool>,
    pub transaction_index: Option<bool>,
}

impl LogFieldSelection {
    pub fn to_cols(self) -> Vec<Expr> {
        let mut cols = Vec::new();

        let table_name = "log";
        append_col!(table_name, cols, self, address);
        append_col!(table_name, cols, self, block_hash);
        append_col!(table_name, cols, self, block_number);
        append_col!(table_name, cols, self, data);
        append_col!(table_name, cols, self, log_index);
        append_col!(table_name, cols, self, removed);
        if let Some(true) = self.topics {
            for i in 0..4 {
                let col = col(&format!("topic{}", i));
                let alias = format!("log_topic{}", i);
                let col = col.alias(&alias);
                cols.push(col);
            }
        }
        append_col!(table_name, cols, self, transaction_hash);
        append_col!(table_name, cols, self, transaction_index);

        cols
    }

    pub fn prune(&self, log: Log) -> ResponseLog {
        ResponseLog {
            address: prune_col!(log, self, address),
            block_hash: prune_col!(log, self, block_hash),
            block_number: prune_col!(log, self, block_number),
            data: prune_col!(log, self, data),
            log_index: prune_col!(log, self, log_index),
            removed: prune_col!(log, self, removed),
            topics: prune_col!(log, self, topics),
            transaction_hash: prune_col!(log, self, transaction_hash),
            transaction_index: prune_col!(log, self, transaction_index),
        }
    }

    pub fn merge(left: Option<Self>, right: Option<Self>) -> Option<Self> {
        let left = match left {
            Some(left) => left,
            None => return right,
        };

        let right = match right {
            Some(right) => right,
            None => return Some(left),
        };

        Some(Self {
            address: merge_opt(left.address, right.address),
            block_hash: merge_opt(left.block_hash, right.block_hash),
            block_number: merge_opt(left.block_number, right.block_number),
            data: merge_opt(left.data, right.data),
            log_index: merge_opt(left.log_index, right.log_index),
            removed: merge_opt(left.removed, right.removed),
            topics: merge_opt(left.topics, right.topics),
            transaction_hash: merge_opt(left.transaction_hash, right.transaction_hash),
            transaction_index: merge_opt(left.transaction_index, right.transaction_index),
        })
    }
}

fn merge_opt(left: Option<bool>, right: Option<bool>) -> Option<bool> {
    let left = match left {
        Some(left) => left,
        None => return right,
    };

    let right = match right {
        Some(right) => right,
        None => return Some(left),
    };

    Some(left || right)
}
