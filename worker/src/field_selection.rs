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
    pub parent_hash: Option<bool>,
    pub sha3_uncles: Option<bool>,
    pub miner: Option<bool>,
    pub state_root: Option<bool>,
    pub transactions_root: Option<bool>,
    pub receipts_root: Option<bool>,
    pub logs_bloom: Option<bool>,
    pub difficulty: Option<bool>,
    pub number: Option<bool>,
    pub gas_limit: Option<bool>,
    pub gas_used: Option<bool>,
    pub timestamp: Option<bool>,
    pub extra_data: Option<bool>,
    pub mix_hash: Option<bool>,
    pub nonce: Option<bool>,
    pub total_difficulty: Option<bool>,
    pub base_fee_per_gas: Option<bool>,
    pub size: Option<bool>,
    pub hash: Option<bool>,
}

impl BlockFieldSelection {
    pub fn to_cols(self) -> Vec<Expr> {
        let mut cols = Vec::new();

        let table_name = "block";
        append_col!(table_name, cols, self, parent_hash);
        append_col!(table_name, cols, self, sha3_uncles);
        append_col!(table_name, cols, self, miner);
        append_col!(table_name, cols, self, state_root);
        append_col!(table_name, cols, self, transactions_root);
        append_col!(table_name, cols, self, receipts_root);
        append_col!(table_name, cols, self, logs_bloom);
        append_col!(table_name, cols, self, difficulty);
        append_col!(table_name, cols, self, number);
        append_col!(table_name, cols, self, gas_limit);
        append_col!(table_name, cols, self, gas_used);
        append_col!(table_name, cols, self, timestamp);
        append_col!(table_name, cols, self, extra_data);
        append_col!(table_name, cols, self, mix_hash);
        append_col!(table_name, cols, self, nonce);
        append_col!(table_name, cols, self, total_difficulty);
        append_col!(table_name, cols, self, base_fee_per_gas);
        append_col!(table_name, cols, self, size);
        append_col!(table_name, cols, self, hash);

        cols
    }

    pub fn prune(&self, block: Block) -> ResponseBlock {
        ResponseBlock {
            parent_hash: prune_col!(block, self, parent_hash),
            sha3_uncles: prune_col!(block, self, sha3_uncles),
            miner: prune_col!(block, self, miner),
            state_root: prune_col!(block, self, state_root),
            transactions_root: prune_col!(block, self, transactions_root),
            receipts_root: prune_col!(block, self, receipts_root),
            logs_bloom: prune_col!(block, self, logs_bloom),
            difficulty: prune_col!(block, self, difficulty).flatten(),
            number: prune_col!(block, self, number),
            gas_limit: prune_col!(block, self, gas_limit),
            gas_used: prune_col!(block, self, gas_used),
            timestamp: prune_col!(block, self, timestamp),
            extra_data: prune_col!(block, self, extra_data),
            mix_hash: prune_col!(block, self, mix_hash).flatten(),
            nonce: prune_col!(block, self, nonce).flatten(),
            total_difficulty: prune_col!(block, self, total_difficulty).flatten(),
            base_fee_per_gas: prune_col!(block, self, base_fee_per_gas).flatten(),
            size: prune_col!(block, self, size),
            hash: prune_col!(block, self, hash).flatten(),
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
            parent_hash: merge_opt(left.parent_hash, right.parent_hash),
            sha3_uncles: merge_opt(left.sha3_uncles, right.sha3_uncles),
            miner: merge_opt(left.miner, right.miner),
            state_root: merge_opt(left.state_root, right.state_root),
            transactions_root: merge_opt(left.transactions_root, right.transactions_root),
            receipts_root: merge_opt(left.receipts_root, right.receipts_root),
            logs_bloom: merge_opt(left.logs_bloom, right.logs_bloom),
            difficulty: merge_opt(left.difficulty, right.difficulty),
            number: merge_opt(left.number, right.number),
            gas_limit: merge_opt(left.gas_limit, right.gas_limit),
            gas_used: merge_opt(left.gas_used, right.gas_used),
            timestamp: merge_opt(left.timestamp, right.timestamp),
            extra_data: merge_opt(left.extra_data, right.extra_data),
            mix_hash: merge_opt(left.mix_hash, right.mix_hash),
            nonce: merge_opt(left.nonce, right.nonce),
            total_difficulty: merge_opt(left.total_difficulty, right.total_difficulty),
            base_fee_per_gas: merge_opt(left.base_fee_per_gas, right.base_fee_per_gas),
            size: merge_opt(left.size, right.size),
            hash: merge_opt(left.hash, right.hash),
        })
    }
}

#[derive(Deserialize, Debug, Clone, Copy, Default)]
#[serde(rename_all = "camelCase")]
pub struct TransactionFieldSelection {
    #[serde(rename = "type")]
    pub kind: Option<bool>,
    pub nonce: Option<bool>,
    #[serde(rename = "to")]
    pub dest: Option<bool>,
    pub gas: Option<bool>,
    pub value: Option<bool>,
    pub input: Option<bool>,
    pub max_priority_fee_per_gas: Option<bool>,
    pub max_fee_per_gas: Option<bool>,
    pub y_parity: Option<bool>,
    pub chain_id: Option<bool>,
    pub v: Option<bool>,
    pub r: Option<bool>,
    pub s: Option<bool>,
    #[serde(rename = "from")]
    pub source: Option<bool>,
    pub block_hash: Option<bool>,
    pub block_number: Option<bool>,
    #[serde(rename = "index")]
    pub transaction_index: Option<bool>,
    pub gas_price: Option<bool>,
    pub hash: Option<bool>,
}

impl TransactionFieldSelection {
    pub fn to_cols(self) -> Vec<Expr> {
        let mut cols = Vec::new();

        let table_name = "tx";
        append_col!(table_name, cols, self, kind);
        append_col!(table_name, cols, self, nonce);
        append_col!(table_name, cols, self, dest);
        append_col!(table_name, cols, self, gas);
        append_col!(table_name, cols, self, value);
        append_col!(table_name, cols, self, input);
        append_col!(table_name, cols, self, max_priority_fee_per_gas);
        append_col!(table_name, cols, self, max_fee_per_gas);
        append_col!(table_name, cols, self, y_parity);
        append_col!(table_name, cols, self, chain_id);
        append_col!(table_name, cols, self, v);
        append_col!(table_name, cols, self, r);
        append_col!(table_name, cols, self, s);
        append_col!(table_name, cols, self, source);
        append_col!(table_name, cols, self, block_hash);
        append_col!(table_name, cols, self, block_number);
        append_col!(table_name, cols, self, transaction_index);
        append_col!(table_name, cols, self, gas_price);
        append_col!(table_name, cols, self, hash);

        cols
    }

    pub fn prune(&self, tx: Transaction) -> ResponseTransaction {
        ResponseTransaction {
            kind: prune_col!(tx, self, kind),
            nonce: prune_col!(tx, self, nonce),
            dest: prune_col!(tx, self, dest).flatten(),
            gas: prune_col!(tx, self, gas),
            value: prune_col!(tx, self, value),
            input: prune_col!(tx, self, input),
            max_priority_fee_per_gas: prune_col!(tx, self, max_priority_fee_per_gas).flatten(),
            max_fee_per_gas: prune_col!(tx, self, max_fee_per_gas).flatten(),
            y_parity: prune_col!(tx, self, y_parity).flatten(),
            chain_id: prune_col!(tx, self, chain_id).flatten(),
            v: prune_col!(tx, self, v).flatten(),
            r: prune_col!(tx, self, r),
            s: prune_col!(tx, self, s),
            source: prune_col!(tx, self, source).flatten(),
            block_hash: prune_col!(tx, self, block_hash),
            block_number: prune_col!(tx, self, block_number),
            transaction_index: prune_col!(tx, self, transaction_index),
            gas_price: prune_col!(tx, self, gas_price).flatten(),
            hash: prune_col!(tx, self, hash),
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
            kind: merge_opt(left.kind, right.kind),
            nonce: merge_opt(left.nonce, right.nonce),
            dest: merge_opt(left.dest, right.dest),
            gas: merge_opt(left.gas, right.gas),
            value: merge_opt(left.value, right.value),
            input: merge_opt(left.input, right.input),
            max_priority_fee_per_gas: merge_opt(
                left.max_priority_fee_per_gas,
                right.max_priority_fee_per_gas,
            ),
            max_fee_per_gas: merge_opt(left.max_fee_per_gas, right.max_fee_per_gas),
            y_parity: merge_opt(left.y_parity, right.y_parity),
            chain_id: merge_opt(left.chain_id, right.chain_id),
            v: merge_opt(left.v, right.v),
            r: merge_opt(left.r, right.r),
            s: merge_opt(left.s, right.s),
            source: merge_opt(left.source, right.source),
            block_hash: merge_opt(left.block_hash, right.block_hash),
            block_number: merge_opt(left.block_number, right.block_number),
            transaction_index: merge_opt(left.transaction_index, right.transaction_index),
            gas_price: merge_opt(left.gas_price, right.gas_price),
            hash: merge_opt(left.hash, right.hash),
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
