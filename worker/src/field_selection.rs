use eth_archive_core::hash::HashSet;
use eth_archive_core::types::{
    Block, Log, ResponseBlock, ResponseLog, ResponseTransaction, Transaction,
};
use serde::{Deserialize, Serialize};

macro_rules! to_fields {
    ($self:expr, $fields:expr, $field:ident) => {
        if $self.$field {
            $fields.insert(stringify!($field));
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

macro_rules! prune_col_opt {
    ($src:ident, $self:ident, $field:ident) => {
        if $self.$field {
            $src.$field
        } else {
            None
        }
    };
}

#[derive(
    Serialize,
    Deserialize,
    Debug,
    Clone,
    Copy,
    Default,
    derive_more::BitOr,
    derive_more::Not,
    PartialEq,
    Eq,
)]
#[serde(rename_all = "camelCase")]
#[serde(default)]
pub struct FieldSelection {
    pub block: BlockFieldSelection,
    pub transaction: TransactionFieldSelection,
    pub log: LogFieldSelection,
}

#[derive(
    Serialize,
    Deserialize,
    Debug,
    Clone,
    Copy,
    Default,
    derive_more::BitOr,
    derive_more::Not,
    PartialEq,
    Eq,
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
    pub fn as_fields(&self) -> HashSet<&'static str> {
        let mut fields = HashSet::new();

        to_fields!(self, fields, parent_hash);
        to_fields!(self, fields, sha3_uncles);
        to_fields!(self, fields, miner);
        to_fields!(self, fields, state_root);
        to_fields!(self, fields, transactions_root);
        to_fields!(self, fields, receipts_root);
        to_fields!(self, fields, logs_bloom);
        to_fields!(self, fields, difficulty);
        to_fields!(self, fields, number);
        to_fields!(self, fields, gas_limit);
        to_fields!(self, fields, gas_used);
        to_fields!(self, fields, timestamp);
        to_fields!(self, fields, extra_data);
        to_fields!(self, fields, mix_hash);
        to_fields!(self, fields, nonce);
        to_fields!(self, fields, total_difficulty);
        to_fields!(self, fields, base_fee_per_gas);
        to_fields!(self, fields, size);
        to_fields!(self, fields, hash);

        fields
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

    pub fn prune_opt(&self, block: ResponseBlock) -> ResponseBlock {
        ResponseBlock {
            parent_hash: prune_col_opt!(block, self, parent_hash),
            sha3_uncles: prune_col_opt!(block, self, sha3_uncles),
            miner: prune_col_opt!(block, self, miner),
            state_root: prune_col_opt!(block, self, state_root),
            transactions_root: prune_col_opt!(block, self, transactions_root),
            receipts_root: prune_col_opt!(block, self, receipts_root),
            logs_bloom: prune_col_opt!(block, self, logs_bloom),
            difficulty: prune_col_opt!(block, self, difficulty),
            number: prune_col_opt!(block, self, number),
            gas_limit: prune_col_opt!(block, self, gas_limit),
            gas_used: prune_col_opt!(block, self, gas_used),
            timestamp: prune_col_opt!(block, self, timestamp),
            extra_data: prune_col_opt!(block, self, extra_data),
            mix_hash: prune_col_opt!(block, self, mix_hash),
            nonce: prune_col_opt!(block, self, nonce),
            total_difficulty: prune_col_opt!(block, self, total_difficulty),
            base_fee_per_gas: prune_col_opt!(block, self, base_fee_per_gas),
            size: prune_col_opt!(block, self, size),
            hash: prune_col_opt!(block, self, hash),
        }
    }
}

#[derive(
    Serialize,
    Deserialize,
    Debug,
    Clone,
    Copy,
    Default,
    derive_more::BitOr,
    derive_more::Not,
    PartialEq,
    Eq,
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
    pub fn as_fields(&self) -> HashSet<&'static str> {
        let mut fields = HashSet::new();

        to_fields!(self, fields, kind);
        to_fields!(self, fields, nonce);
        to_fields!(self, fields, dest);
        to_fields!(self, fields, gas);
        to_fields!(self, fields, value);
        to_fields!(self, fields, input);
        to_fields!(self, fields, max_priority_fee_per_gas);
        to_fields!(self, fields, max_fee_per_gas);
        to_fields!(self, fields, y_parity);
        to_fields!(self, fields, chain_id);
        to_fields!(self, fields, v);
        to_fields!(self, fields, r);
        to_fields!(self, fields, s);
        to_fields!(self, fields, source);
        to_fields!(self, fields, block_hash);
        to_fields!(self, fields, block_number);
        to_fields!(self, fields, transaction_index);
        to_fields!(self, fields, gas_price);
        to_fields!(self, fields, hash);
        to_fields!(self, fields, status);

        fields
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

    pub fn prune_opt(&self, tx: ResponseTransaction) -> ResponseTransaction {
        ResponseTransaction {
            kind: prune_col_opt!(tx, self, kind),
            nonce: prune_col_opt!(tx, self, nonce),
            dest: prune_col_opt!(tx, self, dest),
            gas: prune_col_opt!(tx, self, gas),
            value: prune_col_opt!(tx, self, value),
            input: prune_col_opt!(tx, self, input),
            max_priority_fee_per_gas: prune_col_opt!(tx, self, max_priority_fee_per_gas),
            max_fee_per_gas: prune_col_opt!(tx, self, max_fee_per_gas),
            y_parity: prune_col_opt!(tx, self, y_parity),
            chain_id: prune_col_opt!(tx, self, chain_id),
            v: prune_col_opt!(tx, self, v),
            r: prune_col_opt!(tx, self, r),
            s: prune_col_opt!(tx, self, s),
            source: prune_col_opt!(tx, self, source),
            block_hash: prune_col_opt!(tx, self, block_hash),
            block_number: prune_col_opt!(tx, self, block_number),
            transaction_index: prune_col_opt!(tx, self, transaction_index),
            gas_price: prune_col_opt!(tx, self, gas_price),
            hash: prune_col_opt!(tx, self, hash),
            status: prune_col_opt!(tx, self, status),
        }
    }
}

#[derive(
    Serialize,
    Deserialize,
    Debug,
    Clone,
    Copy,
    Default,
    derive_more::BitOr,
    derive_more::Not,
    PartialEq,
    Eq,
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
    pub fn as_fields(&self) -> HashSet<&'static str> {
        let mut fields = HashSet::new();

        to_fields!(self, fields, address);
        to_fields!(self, fields, block_hash);
        to_fields!(self, fields, block_number);
        to_fields!(self, fields, data);
        to_fields!(self, fields, log_index);
        to_fields!(self, fields, removed);
        to_fields!(self, fields, topics);
        to_fields!(self, fields, transaction_hash);
        to_fields!(self, fields, transaction_index);

        fields
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

    pub fn prune_opt(&self, log: ResponseLog) -> ResponseLog {
        ResponseLog {
            address: prune_col_opt!(log, self, address),
            block_hash: prune_col_opt!(log, self, block_hash),
            block_number: prune_col_opt!(log, self, block_number),
            data: prune_col_opt!(log, self, data),
            log_index: prune_col_opt!(log, self, log_index),
            removed: prune_col_opt!(log, self, removed),
            topics: prune_col_opt!(log, self, topics),
            transaction_hash: prune_col_opt!(log, self, transaction_hash),
            transaction_index: prune_col_opt!(log, self, transaction_index),
        }
    }
}
