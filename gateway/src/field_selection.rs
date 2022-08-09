use convert_case::{Case, Casing};
use datafusion::prelude::*;
use serde::Deserialize;

macro_rules! append_col {
    ($table_name:expr, $cols:ident, $self:ident, $field:ident) => {
        if let Some(true) = $self.$field {
            let field_name = stringify!($field);
            let col = col(&format!("{}.{}", $table_name, field_name));
            let field_name = field_name.to_case(Case::Camel);
            let alias = format!("{}_{}", $table_name, field_name);
            let alias = alias;
            let col = col.alias(&alias);
            $cols.push(col);
        }
    };
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BlockFieldSelection {
    number: Option<bool>,
    hash: Option<bool>,
    parent_hash: Option<bool>,
    nonce: Option<bool>,
    sha3_uncles: Option<bool>,
    logs_bloom: Option<bool>,
    transactions_root: Option<bool>,
    state_root: Option<bool>,
    receipts_root: Option<bool>,
    miner: Option<bool>,
    difficulty: Option<bool>,
    total_difficulty: Option<bool>,
    extra_data: Option<bool>,
    size: Option<bool>,
    gas_limit: Option<bool>,
    gas_used: Option<bool>,
    timestamp: Option<bool>,
}

impl BlockFieldSelection {
    pub fn to_cols(&self) -> Vec<Expr> {
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
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransactionFieldSelection {
    block_hash: Option<bool>,
    block_number: Option<bool>,
    from: Option<bool>,
    gas: Option<bool>,
    gas_price: Option<bool>,
    hash: Option<bool>,
    input: Option<bool>,
    nonce: Option<bool>,
    to: Option<bool>,
    transaction_index: Option<bool>,
    value: Option<bool>,
    v: Option<bool>,
    r: Option<bool>,
    s: Option<bool>,
}

impl TransactionFieldSelection {
    pub fn to_cols(&self) -> Vec<Expr> {
        let mut cols = Vec::new();
        let table_name = "tx";
        append_col!(table_name, cols, self, block_hash);
        append_col!(table_name, cols, self, block_number);
        append_col!(table_name, cols, self, from);
        append_col!(table_name, cols, self, gas);
        append_col!(table_name, cols, self, gas_price);
        append_col!(table_name, cols, self, hash);
        append_col!(table_name, cols, self, input);
        append_col!(table_name, cols, self, nonce);
        append_col!(table_name, cols, self, to);
        append_col!(table_name, cols, self, transaction_index);
        append_col!(table_name, cols, self, value);
        append_col!(table_name, cols, self, v);
        append_col!(table_name, cols, self, r);
        append_col!(table_name, cols, self, s);

        cols
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LogFieldSelection {
    block: BlockFieldSelection,
    tx: TransactionFieldSelection,
    address: Option<bool>,
    block_hash: Option<bool>,
    block_number: Option<bool>,
    data: Option<bool>,
    log_index: Option<bool>,
    removed: Option<bool>,
    topic0: Option<bool>,
    topic1: Option<bool>,
    topic2: Option<bool>,
    topic3: Option<bool>,
    transaction_hash: Option<bool>,
    transaction_index: Option<bool>,
}

impl LogFieldSelection {
    pub fn to_cols(&self) -> Vec<Expr> {
        let mut cols = Vec::new();
        let table_name = "log";
        append_col!(table_name, cols, self, address);
        append_col!(table_name, cols, self, block_hash);
        append_col!(table_name, cols, self, block_number);
        append_col!(table_name, cols, self, data);
        append_col!(table_name, cols, self, log_index);
        append_col!(table_name, cols, self, removed);
        append_col!(table_name, cols, self, topic0);
        append_col!(table_name, cols, self, topic1);
        append_col!(table_name, cols, self, topic2);
        append_col!(table_name, cols, self, topic3);
        append_col!(table_name, cols, self, transaction_hash);
        append_col!(table_name, cols, self, transaction_index);

        cols.extend_from_slice(&self.tx.to_cols());
        cols.extend_from_slice(&self.block.to_cols());

        cols
    }
}
