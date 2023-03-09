use super::util::{define_cols, map_from_arrow, map_from_arrow_opt};
use super::{Columns, ParquetSource};
use crate::deserialize::{Address, BigUnsigned, BloomFilterBytes, Bytes, Bytes32, Index};
use crate::types::{Block, Log, Transaction};
use arrayvec::ArrayVec;
use arrow2::array::{self, BooleanArray, UInt32Array, UInt64Array};
use arrow2::compute::cast::cast;
use arrow2::compute::concatenate::concatenate;
use arrow2::datatypes::{DataType, Field};
use std::collections::BTreeMap;

type BinaryArray = array::BinaryArray<i64>;

pub struct Ver0_1_0;

impl ParquetSource for Ver0_1_0 {
    fn read_blocks(&self, columns: Columns) -> BTreeMap<u32, Block> {
        let mut blocks = BTreeMap::new();

        for columns in columns {
            #[rustfmt::skip]
            define_cols!(
                columns,
                block_number, UInt32Array,
                block_parent_hash, BinaryArray,
                block_sha3_uncles, BinaryArray,
                block_miner, BinaryArray,
                block_state_root, BinaryArray,
                block_transactions_root, BinaryArray,
                block_receipts_root, BinaryArray,
                block_logs_bloom, BinaryArray,
                block_difficulty, BinaryArray,
                block_gas_limit, BinaryArray,
                block_gas_used, BinaryArray,
                block_timestamp, BinaryArray,
                block_extra_data, BinaryArray,
                block_mix_hash, BinaryArray,
                block_nonce, UInt64Array,
                block_total_difficulty, BinaryArray,
                block_base_fee_per_gas, BinaryArray,
                block_size, BinaryArray,
                block_hash, BinaryArray
            );

            let len = block_number.len();

            for i in 0..len {
                let number = map_from_arrow!(block_number, Index, i);

                blocks.insert(
                    number.0,
                    Block {
                        parent_hash: map_from_arrow!(block_parent_hash, Bytes32::new, i),
                        sha3_uncles: map_from_arrow!(block_sha3_uncles, Bytes32::new, i),
                        miner: map_from_arrow!(block_miner, Address::new, i),
                        state_root: map_from_arrow!(block_state_root, Bytes32::new, i),
                        transactions_root: map_from_arrow!(
                            block_transactions_root,
                            Bytes32::new,
                            i
                        ),
                        receipts_root: map_from_arrow!(block_receipts_root, Bytes32::new, i),
                        logs_bloom: map_from_arrow!(block_logs_bloom, BloomFilterBytes::new, i),
                        difficulty: map_from_arrow_opt!(block_difficulty, Bytes::new, i),
                        number,
                        gas_limit: map_from_arrow!(block_gas_limit, Bytes::new, i),
                        gas_used: map_from_arrow!(block_gas_used, Bytes::new, i),
                        timestamp: map_from_arrow!(block_timestamp, Bytes::new, i),
                        extra_data: map_from_arrow!(block_extra_data, Bytes::new, i),
                        mix_hash: map_from_arrow_opt!(block_mix_hash, Bytes32::new, i),
                        nonce: map_from_arrow_opt!(block_nonce, BigUnsigned, i),
                        total_difficulty: map_from_arrow_opt!(
                            block_total_difficulty,
                            Bytes::new,
                            i
                        ),
                        base_fee_per_gas: map_from_arrow_opt!(
                            block_base_fee_per_gas,
                            Bytes::new,
                            i
                        ),
                        size: map_from_arrow!(block_size, Bytes::new, i),
                        hash: map_from_arrow_opt!(block_hash, Bytes32::new, i),
                        transactions: Vec::new(),
                    },
                );
            }
        }

        blocks
    }

    fn read_txs(&self, columns: Columns) -> Vec<Transaction> {
        let mut txs = Vec::new();

        for columns in columns {
            #[rustfmt::skip]
            define_cols!(
                columns,
                tx_kind, UInt32Array,
                tx_nonce, UInt64Array,
                tx_dest, BinaryArray,
                tx_gas, BinaryArray,
                tx_value, BinaryArray,
                tx_input, BinaryArray,
                tx_max_priority_fee_per_gas, BinaryArray,
                tx_max_fee_per_gas, BinaryArray,
                tx_y_parity, UInt32Array,
                tx_chain_id, UInt32Array,
                tx_v, UInt64Array,
                tx_r, BinaryArray,
                tx_s, BinaryArray,
                tx_source, BinaryArray,
                tx_block_hash, BinaryArray,
                tx_block_number, UInt32Array,
                tx_transaction_index, UInt32Array,
                tx_gas_price, BinaryArray,
                tx_hash, BinaryArray,
                tx_status, UInt32Array
            );

            let len = tx_block_number.len();

            for i in 0..len {
                txs.push(Transaction {
                    kind: map_from_arrow_opt!(tx_kind, Index, i),
                    nonce: map_from_arrow!(tx_nonce, BigUnsigned, i),
                    dest: map_from_arrow_opt!(tx_dest, Address::new, i),
                    gas: map_from_arrow!(tx_gas, Bytes::new, i),
                    value: map_from_arrow!(tx_value, Bytes::new, i),
                    input: map_from_arrow!(tx_input, Bytes::new, i),
                    max_priority_fee_per_gas: map_from_arrow_opt!(
                        tx_max_priority_fee_per_gas,
                        Bytes::new,
                        i
                    ),
                    max_fee_per_gas: map_from_arrow_opt!(tx_max_fee_per_gas, Bytes::new, i),
                    y_parity: map_from_arrow_opt!(tx_y_parity, Index, i),
                    chain_id: map_from_arrow_opt!(tx_chain_id, Index, i),
                    v: map_from_arrow_opt!(tx_v, BigUnsigned, i),
                    r: map_from_arrow!(tx_r, Bytes::new, i),
                    s: map_from_arrow!(tx_s, Bytes::new, i),
                    source: map_from_arrow_opt!(tx_source, Address::new, i),
                    block_hash: map_from_arrow!(tx_block_hash, Bytes32::new, i),
                    block_number: map_from_arrow!(tx_block_number, Index, i),
                    transaction_index: map_from_arrow!(tx_transaction_index, Index, i),
                    gas_price: map_from_arrow_opt!(tx_gas_price, Bytes::new, i),
                    hash: map_from_arrow!(tx_hash, Bytes32::new, i),
                    status: map_from_arrow_opt!(tx_status, Index, i),
                });
            }
        }

        txs
    }

    fn read_logs(&self, columns: Columns) -> Vec<Log> {
        let mut logs = Vec::new();

        for columns in columns {
            #[rustfmt::skip]
            define_cols!(
                columns,
                log_address, BinaryArray,
                log_block_hash, BinaryArray,
                log_block_number, UInt32Array,
                log_data, BinaryArray,
                log_log_index, UInt32Array,
                log_removed, BooleanArray,
                log_topic0, BinaryArray,
                log_topic1, BinaryArray,
                log_topic2, BinaryArray,
                log_topic3, BinaryArray,
                log_transaction_hash, BinaryArray,
                log_transaction_index, UInt32Array
            );

            let len = log_block_number.len();

            for i in 0..len {
                logs.push(Log {
                    address: map_from_arrow!(log_address, Address::new, i),
                    block_hash: map_from_arrow!(log_block_hash, Bytes32::new, i),
                    block_number: map_from_arrow!(log_block_number, Index, i),
                    data: map_from_arrow!(log_data, Bytes::new, i),
                    log_index: map_from_arrow!(log_log_index, Index, i),
                    removed: log_removed.get(i),
                    topics: {
                        let mut topics = ArrayVec::new();

                        if let Some(topic) = log_topic0.get(i) {
                            topics.push(Bytes32::new(topic));
                        }

                        if let Some(topic) = log_topic1.get(i) {
                            topics.push(Bytes32::new(topic));
                        }

                        if let Some(topic) = log_topic2.get(i) {
                            topics.push(Bytes32::new(topic));
                        }

                        if let Some(topic) = log_topic3.get(i) {
                            topics.push(Bytes32::new(topic));
                        }

                        topics
                    },
                    transaction_hash: map_from_arrow!(log_transaction_hash, Bytes32::new, i),
                    transaction_index: map_from_arrow!(log_transaction_index, Index, i),
                });
            }
        }

        logs
    }

    fn block_fields(&self) -> Vec<Field> {
        vec![
            Field::new("parent_hash", DataType::Binary, false),
            Field::new("sha3_uncles", DataType::Binary, false),
            Field::new("miner", DataType::Binary, false),
            Field::new("state_root", DataType::Binary, false),
            Field::new("transactions_root", DataType::Binary, false),
            Field::new("receipts_root", DataType::Binary, false),
            Field::new("logs_bloom", DataType::Binary, false),
            Field::new("difficulty", DataType::Binary, true),
            Field::new("number", DataType::UInt32, false),
            Field::new("gas_limit", DataType::Binary, false),
            Field::new("gas_used", DataType::Binary, false),
            Field::new("timestamp", DataType::Int64, false),
            Field::new("extra_data", DataType::Binary, false),
            Field::new("mix_hash", DataType::Binary, true),
            Field::new("nonce", DataType::UInt64, true),
            Field::new("total_difficulty", DataType::Binary, true),
            Field::new("base_fee_per_gas", DataType::Binary, true),
            Field::new("size", DataType::Int64, false),
            Field::new("hash", DataType::Binary, true),
        ]
    }

    fn tx_fields(&self) -> Vec<Field> {
        vec![
            Field::new("kind", DataType::UInt32, false),
            Field::new("nonce", DataType::UInt64, false),
            Field::new("dest", DataType::Binary, true),
            Field::new("gas", DataType::Int64, false),
            Field::new("value", DataType::Binary, false),
            Field::new("input", DataType::Binary, false),
            Field::new("max_priority_fee_per_gas", DataType::Int64, true),
            Field::new("max_fee_per_gas", DataType::Int64, true),
            Field::new("y_parity", DataType::UInt32, true),
            Field::new("chain_id", DataType::UInt32, true),
            Field::new("v", DataType::Int64, true),
            Field::new("r", DataType::Binary, false),
            Field::new("s", DataType::Binary, false),
            Field::new("source", DataType::Binary, true),
            Field::new("block_hash", DataType::Binary, false),
            Field::new("block_number", DataType::UInt32, false),
            Field::new("transaction_index", DataType::UInt32, false),
            Field::new("gas_price", DataType::Int64, true),
            Field::new("hash", DataType::Binary, false),
            Field::new("sighash", DataType::Binary, true),
        ]
    }

    fn log_fields(&self) -> Vec<Field> {
        vec![
            Field::new("address", DataType::Binary, false),
            Field::new("block_hash", DataType::Binary, false),
            Field::new("block_number", DataType::UInt32, false),
            Field::new("data", DataType::Binary, false),
            Field::new("log_index", DataType::UInt32, false),
            Field::new("removed", DataType::Boolean, false),
            Field::new("topic0", DataType::Binary, true),
            Field::new("topic1", DataType::Binary, true),
            Field::new("topic2", DataType::Binary, true),
            Field::new("topic3", DataType::Binary, true),
            Field::new("transaction_hash", DataType::Binary, false),
            Field::new("transaction_index", DataType::UInt32, false),
        ]
    }
}
