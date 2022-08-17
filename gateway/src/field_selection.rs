use datafusion::prelude::*;
use serde::Deserialize;

macro_rules! append_col {
    ($table_name:expr, $cols:ident, $self:ident, $field:ident) => {
        if let Some(true) = $self.$field {
            let field_name = stringify!($field);
            let col = col(&format!("{}.{}", $table_name, field_name));
            let alias = format!("{}_{}", $table_name, field_name);
            let col = col.alias(&alias);
            $cols.push(col);
        }
    };
}

macro_rules! append_col_sql {
    ($table_name:expr, $cols:ident, $self:ident, $field:ident) => {
        if let Some(true) = $self.$field {
            let field_name = stringify!($field);
            let col = format!("{}.{}", $table_name, field_name);
            let alias = format!("{}_{}", $table_name, field_name);
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
        let table_name = "eth_block";
        append_col_sql!(table_name, cols, self, number);
        append_col_sql!(table_name, cols, self, hash);
        append_col_sql!(table_name, cols, self, parent_hash);
        append_col_sql!(table_name, cols, self, nonce);
        append_col_sql!(table_name, cols, self, sha3_uncles);
        append_col_sql!(table_name, cols, self, logs_bloom);
        append_col_sql!(table_name, cols, self, transactions_root);
        append_col_sql!(table_name, cols, self, state_root);
        append_col_sql!(table_name, cols, self, receipts_root);
        append_col_sql!(table_name, cols, self, miner);
        append_col_sql!(table_name, cols, self, difficulty);
        append_col_sql!(table_name, cols, self, total_difficulty);
        append_col_sql!(table_name, cols, self, extra_data);
        append_col_sql!(table_name, cols, self, size);
        append_col_sql!(table_name, cols, self, gas_limit);
        append_col_sql!(table_name, cols, self, gas_used);
        append_col_sql!(table_name, cols, self, timestamp);
    }

    pub fn to_cols(&self, cols: &mut Vec<Expr>) {
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
        let table_name = "eth_tx";
        append_col_sql!(table_name, cols, self, block_hash);
        append_col_sql!(table_name, cols, self, block_number);
        append_col_sql!(table_name, cols, self, source);
        append_col_sql!(table_name, cols, self, gas);
        append_col_sql!(table_name, cols, self, gas_price);
        append_col_sql!(table_name, cols, self, hash);
        append_col_sql!(table_name, cols, self, input);
        append_col_sql!(table_name, cols, self, nonce);
        append_col_sql!(table_name, cols, self, dest);
        append_col_sql!(table_name, cols, self, transaction_index);
        append_col_sql!(table_name, cols, self, value);
        append_col_sql!(table_name, cols, self, kind);
        append_col_sql!(table_name, cols, self, chain_id);
        append_col_sql!(table_name, cols, self, v);
        append_col_sql!(table_name, cols, self, r);
        append_col_sql!(table_name, cols, self, s);
    }

    pub fn to_cols(&self, cols: &mut Vec<Expr>) {
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
    topics: Option<bool>,
    transaction_hash: Option<bool>,
    transaction_index: Option<bool>,
}

impl LogFieldSelection {
    pub fn to_cols_sql(&self, cols: &mut Vec<String>) {
        let table_name = "eth_log";
        append_col_sql!(table_name, cols, self, address);
        append_col_sql!(table_name, cols, self, block_hash);
        append_col_sql!(table_name, cols, self, block_number);
        append_col_sql!(table_name, cols, self, data);
        append_col_sql!(table_name, cols, self, log_index);
        append_col_sql!(table_name, cols, self, removed);
        if let Some(true) = self.topics {
            for i in 0..4 {
                cols.push(format!("log.topic{0} as log_topic{0}", i));
            }
        }
        append_col_sql!(table_name, cols, self, transaction_hash);
        append_col_sql!(table_name, cols, self, transaction_index);
    }

    pub fn to_cols(&self, cols: &mut Vec<Expr>) {
        let table_name = "log";
        append_col!(table_name, cols, self, address);
        append_col!(table_name, cols, self, block_hash);
        append_col!(table_name, cols, self, block_number);
        append_col!(table_name, cols, self, data);
        append_col!(table_name, cols, self, log_index);
        append_col!(table_name, cols, self, removed);
        if let Some(true) = self.topics {
            for i in 0..4 {
                let col = col(&format!("log.topic{}", i));
                let alias = format!("log_topic{}", i);
                let col = col.alias(&alias);
                cols.push(col);
            }
        }
        append_col!(table_name, cols, self, transaction_hash);
        append_col!(table_name, cols, self, transaction_index);
    }
}
