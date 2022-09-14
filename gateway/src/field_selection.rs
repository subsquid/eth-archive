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

#[derive(Deserialize, Debug, Clone, Copy, Default)]
#[serde(rename_all = "camelCase")]
pub struct FieldSelection {
    pub block: Option<BlockFieldSelection>,
    pub transaction: Option<TransactionFieldSelection>,
    log: Option<LogFieldSelection>,
}

impl FieldSelection {
    pub fn to_cols_sql(self) -> String {
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

    pub fn to_cols(self) -> Vec<Expr> {
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
    pub fn to_cols_sql(self, cols: &mut Vec<String>) {
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

    pub fn to_cols(self, cols: &mut Vec<Expr>) {
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
    block_number: Option<bool>,
    #[serde(alias = "from")]
    source: Option<bool>,
    gas: Option<bool>,
    gas_price: Option<bool>,
    pub hash: Option<bool>,
    input: Option<bool>,
    nonce: Option<bool>,
    #[serde(alias = "to")]
    dest: Option<bool>,
    #[serde(alias = "index")]
    transaction_index: Option<bool>,
    value: Option<bool>,
    kind: Option<bool>,
    chain_id: Option<bool>,
    v: Option<bool>,
    r: Option<bool>,
    s: Option<bool>,
}

impl TransactionFieldSelection {
    pub fn to_cols_sql(self, cols: &mut Vec<String>) {
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

    pub fn to_cols(self, cols: &mut Vec<Expr>) {
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
    address: Option<bool>,
    block_hash: Option<bool>,
    block_number: Option<bool>,
    data: Option<bool>,
    #[serde(alias = "index")]
    log_index: Option<bool>,
    removed: Option<bool>,
    topics: Option<bool>,
    transaction_hash: Option<bool>,
    transaction_index: Option<bool>,
}

impl LogFieldSelection {
    pub fn to_cols_sql(self, cols: &mut Vec<String>) {
        let table_name = "eth_log";
        append_col_sql!(table_name, cols, self, address);
        append_col_sql!(table_name, cols, self, block_hash);
        append_col_sql!(table_name, cols, self, block_number);
        append_col_sql!(table_name, cols, self, data);
        append_col_sql!(table_name, cols, self, log_index);
        append_col_sql!(table_name, cols, self, removed);
        if let Some(true) = self.topics {
            for i in 0..4 {
                cols.push(format!("eth_log.topic{0} as eth_log_topic{0}", i));
            }
        }
        append_col_sql!(table_name, cols, self, transaction_hash);
        append_col_sql!(table_name, cols, self, transaction_index);
    }

    pub fn to_cols(self, cols: &mut Vec<Expr>) {
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
