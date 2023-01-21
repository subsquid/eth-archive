use eth_archive_core::types::{
    Block, Log, ResponseBlock, ResponseLog, ResponseTransaction, Transaction,
};
use polars::prelude::*;
use serde::{Deserialize, Serialize};

macro_rules! append_col {
    ($table_name:expr, $cols:ident, $self:ident, $field:ident) => {
        if $self.$field {
            let field_name = stringify!($field);
            let col = col(&format!("{}", field_name));
            let col = col.prefix(&format!("{}_", $table_name));
            $cols.push(col);
        }
    };
}

macro_rules! prune_col {
    ($src:ident, $self:ident, $field:ident) => {
        if $self.$field {
            Some($src.$field)
        } else {
            None
        }
    };
}

#[derive(
    Serialize, Deserialize, Debug, Clone, Copy, Default, derive_more::BitOr, derive_more::Not,
)]
#[serde(rename_all = "camelCase")]
#[serde(default)]
pub struct FieldSelection {
    pub block: BlockFieldSelection,
    pub transaction: TransactionFieldSelection,
    pub log: LogFieldSelection,
}

#[derive(
    Serialize, Deserialize, Debug, Clone, Copy, Default, derive_more::BitOr, derive_more::Not,
)]
#[serde(rename_all = "camelCase")]
#[serde(default)]
pub struct BlockFieldSelection {
    pub parent_hash: bool,
    pub sha3_uncles: bool,
    pub miner: bool,
    pub state_root: bool,
    pub transactions_root: bool,
    pub receipts_root: bool,
    pub logs_bloom: bool,
    pub difficulty: bool,
    pub number: bool,
    pub gas_limit: bool,
    pub gas_used: bool,
    pub timestamp: bool,
    pub extra_data: bool,
    pub mix_hash: bool,
    pub nonce: bool,
    pub total_difficulty: bool,
    pub base_fee_per_gas: bool,
    pub size: bool,
    pub hash: bool,
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
}

#[derive(
    Serialize, Deserialize, Debug, Clone, Copy, Default, derive_more::BitOr, derive_more::Not,
)]
#[serde(rename_all = "camelCase")]
#[serde(default)]
pub struct TransactionFieldSelection {
    #[serde(rename = "type")]
    pub kind: bool,
    pub nonce: bool,
    #[serde(rename = "to")]
    pub dest: bool,
    pub gas: bool,
    pub value: bool,
    pub input: bool,
    pub max_priority_fee_per_gas: bool,
    pub max_fee_per_gas: bool,
    pub y_parity: bool,
    pub chain_id: bool,
    pub v: bool,
    pub r: bool,
    pub s: bool,
    #[serde(rename = "from")]
    pub source: bool,
    pub block_hash: bool,
    pub block_number: bool,
    #[serde(rename = "index")]
    pub transaction_index: bool,
    pub gas_price: bool,
    pub hash: bool,
    pub status: bool,
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
        append_col!(table_name, cols, self, status);

        cols
    }

    pub fn prune(&self, tx: Transaction) -> ResponseTransaction {
        ResponseTransaction {
            kind: prune_col!(tx, self, kind).flatten(),
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
            status: prune_col!(tx, self, status).flatten(),
        }
    }
}

#[derive(
    Serialize, Deserialize, Debug, Clone, Copy, Default, derive_more::BitOr, derive_more::Not,
)]
#[serde(rename_all = "camelCase")]
#[serde(default)]
pub struct LogFieldSelection {
    pub address: bool,
    pub block_hash: bool,
    pub block_number: bool,
    pub data: bool,
    #[serde(rename = "index")]
    pub log_index: bool,
    pub removed: bool,
    pub topics: bool,
    pub transaction_hash: bool,
    pub transaction_index: bool,
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
        if self.topics {
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
            removed: prune_col!(log, self, removed).flatten(),
            topics: prune_col!(log, self, topics),
            transaction_hash: prune_col!(log, self, transaction_hash),
            transaction_index: prune_col!(log, self, transaction_index),
        }
    }
}
