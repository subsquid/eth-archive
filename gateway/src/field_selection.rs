use convert_case::{Case, Casing};
use datafusion::prelude::*;
use serde::Deserialize;

macro_rules! append_col {
    ($table_name:expr, $cols:ident, $self:ident, $field:ident, $is_hex:expr) => {
        if let Some(true) = $self.$field {
            let field_name = stringify!($field);
            let col = col(&format!("{}.{}", $table_name, field_name));
            let field_name = field_name.to_case(Case::Camel);
            let alias = format!("{}_{}", $table_name, field_name);
            let col = if $is_hex { to_hex(col) } else { col };
            let col = col.alias(&alias);
            $cols.push(col);
        }
    };
}

macro_rules! append_col_sql {
    ($table_name:expr, $cols:ident, $self:ident, $field:ident, $is_hex:expr) => {
        if let Some(true) = $self.$field {
            let field_name = stringify!($field);
            let col = format!("{}.{}", $table_name, field_name);
            let field_name = field_name.to_case(Case::Camel);
            let alias = format!("{}_{}", $table_name, field_name);
            let col = if $is_hex {
                format!("encode({}, 'hex')", col)
            } else {
                col
            };
            let col = format!("{} as {}", col, alias);
            $cols.push(col);
        }
    };
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct FieldSelection {
    block: Option<BlockFieldSelection>,
    transaction: Option<TransactionFieldSelection>,
    log: Option<LogFieldSelection>,
}

impl FieldSelection {
    pub fn to_cols_sql(&self) -> String {
        let mut cols = Vec::new();

        if let Some(block) = &self.block {
            block.to_cols_sql(&mut cols);
        }
        if let Some(transaction) = &self.transaction {
            transaction.to_cols_sql(&mut cols);
        }
        if let Some(log) = &self.log {
            log.to_cols_sql(&mut cols);
        }

        cols.join(",\n")
    }

    pub fn to_cols(&self) -> Vec<Expr> {
        let mut cols = Vec::new();

        if let Some(block) = &self.block {
            block.to_cols(&mut cols);
        }
        if let Some(transaction) = &self.transaction {
            transaction.to_cols(&mut cols);
        }
        if let Some(log) = &self.log {
            log.to_cols(&mut cols);
        }

        cols
    }
}

#[derive(Deserialize, Debug)]
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
    pub fn to_cols_sql(&self, cols: &mut Vec<String>) {
        let table_name = "block";
        append_col_sql!(table_name, cols, self, number, false);
        append_col_sql!(table_name, cols, self, hash, true);
        append_col_sql!(table_name, cols, self, parent_hash, true);
        append_col_sql!(table_name, cols, self, nonce, false);
        append_col_sql!(table_name, cols, self, sha3_uncles, true);
        append_col_sql!(table_name, cols, self, logs_bloom, true);
        append_col_sql!(table_name, cols, self, transactions_root, true);
        append_col_sql!(table_name, cols, self, state_root, true);
        append_col_sql!(table_name, cols, self, receipts_root, true);
        append_col_sql!(table_name, cols, self, miner, true);
        append_col_sql!(table_name, cols, self, difficulty, true);
        append_col_sql!(table_name, cols, self, total_difficulty, true);
        append_col_sql!(table_name, cols, self, extra_data, true);
        append_col_sql!(table_name, cols, self, size, false);
        append_col_sql!(table_name, cols, self, gas_limit, true);
        append_col_sql!(table_name, cols, self, gas_used, true);
        append_col_sql!(table_name, cols, self, timestamp, false);
    }

    pub fn to_cols(&self, cols: &mut Vec<Expr>) {
        let table_name = "block";
        append_col!(table_name, cols, self, number, false);
        append_col!(table_name, cols, self, hash, true);
        append_col!(table_name, cols, self, parent_hash, true);
        append_col!(table_name, cols, self, nonce, false);
        append_col!(table_name, cols, self, sha3_uncles, true);
        append_col!(table_name, cols, self, logs_bloom, true);
        append_col!(table_name, cols, self, transactions_root, true);
        append_col!(table_name, cols, self, state_root, true);
        append_col!(table_name, cols, self, receipts_root, true);
        append_col!(table_name, cols, self, miner, true);
        append_col!(table_name, cols, self, difficulty, true);
        append_col!(table_name, cols, self, total_difficulty, true);
        append_col!(table_name, cols, self, extra_data, true);
        append_col!(table_name, cols, self, size, false);
        append_col!(table_name, cols, self, gas_limit, true);
        append_col!(table_name, cols, self, gas_used, true);
        append_col!(table_name, cols, self, timestamp, false);
    }
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct TransactionFieldSelection {
    block_hash: Option<bool>,
    block_number: Option<bool>,
    #[serde(alias = "from")]
    source: Option<bool>,
    gas: Option<bool>,
    gas_price: Option<bool>,
    hash: Option<bool>,
    input: Option<bool>,
    nonce: Option<bool>,
    #[serde(alias = "to")]
    dest: Option<bool>,
    transaction_index: Option<bool>,
    value: Option<bool>,
    kind: Option<bool>,
    chain_id: Option<bool>,
    v: Option<bool>,
    r: Option<bool>,
    s: Option<bool>,
}

impl TransactionFieldSelection {
    pub fn to_cols_sql(&self, cols: &mut Vec<String>) {
        let table_name = "tx";
        append_col_sql!(table_name, cols, self, block_hash, true);
        append_col_sql!(table_name, cols, self, block_number, false);
        append_col_sql!(table_name, cols, self, source, true);
        append_col_sql!(table_name, cols, self, gas, false);
        append_col_sql!(table_name, cols, self, gas_price, false);
        append_col_sql!(table_name, cols, self, hash, true);
        append_col_sql!(table_name, cols, self, input, true);
        append_col_sql!(table_name, cols, self, nonce, false);
        append_col_sql!(table_name, cols, self, dest, true);
        append_col_sql!(table_name, cols, self, transaction_index, false);
        append_col_sql!(table_name, cols, self, value, true);
        append_col_sql!(table_name, cols, self, kind, false);
        append_col_sql!(table_name, cols, self, chain_id, false);
        append_col_sql!(table_name, cols, self, v, false);
        append_col_sql!(table_name, cols, self, r, true);
        append_col_sql!(table_name, cols, self, s, true);
    }

    pub fn to_cols(&self, cols: &mut Vec<Expr>) {
        let table_name = "tx";
        append_col!(table_name, cols, self, block_hash, true);
        append_col!(table_name, cols, self, block_number, false);
        append_col!(table_name, cols, self, source, true);
        append_col!(table_name, cols, self, gas, false);
        append_col!(table_name, cols, self, gas_price, false);
        append_col!(table_name, cols, self, hash, true);
        append_col!(table_name, cols, self, input, true);
        append_col!(table_name, cols, self, nonce, false);
        append_col!(table_name, cols, self, dest, true);
        append_col!(table_name, cols, self, transaction_index, false);
        append_col!(table_name, cols, self, value, true);
        append_col!(table_name, cols, self, kind, false);
        append_col!(table_name, cols, self, chain_id, false);
        append_col!(table_name, cols, self, v, false);
        append_col!(table_name, cols, self, r, true);
        append_col!(table_name, cols, self, s, true);
    }
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct LogFieldSelection {
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
    pub fn to_cols_sql(&self, cols: &mut Vec<String>) {
        let table_name = "log";
        append_col_sql!(table_name, cols, self, address, true);
        append_col_sql!(table_name, cols, self, block_hash, true);
        append_col_sql!(table_name, cols, self, block_number, false);
        append_col_sql!(table_name, cols, self, data, true);
        append_col_sql!(table_name, cols, self, log_index, false);
        append_col_sql!(table_name, cols, self, removed, false);
        append_col_sql!(table_name, cols, self, topic0, true);
        append_col_sql!(table_name, cols, self, topic1, true);
        append_col_sql!(table_name, cols, self, topic2, true);
        append_col_sql!(table_name, cols, self, topic3, true);
        append_col_sql!(table_name, cols, self, transaction_hash, true);
        append_col_sql!(table_name, cols, self, transaction_index, false);
    }

    pub fn to_cols(&self, cols: &mut Vec<Expr>) {
        let table_name = "log";
        append_col!(table_name, cols, self, address, true);
        append_col!(table_name, cols, self, block_hash, true);
        append_col!(table_name, cols, self, block_number, false);
        append_col!(table_name, cols, self, data, true);
        append_col!(table_name, cols, self, log_index, false);
        append_col!(table_name, cols, self, removed, false);
        append_col!(table_name, cols, self, topic0, true);
        append_col!(table_name, cols, self, topic1, true);
        append_col!(table_name, cols, self, topic2, true);
        append_col!(table_name, cols, self, topic3, true);
        append_col!(table_name, cols, self, transaction_hash, true);
        append_col!(table_name, cols, self, transaction_index, false);
    }
}
