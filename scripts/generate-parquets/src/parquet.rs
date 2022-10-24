use eth_archive_core::types::BlockRange;
use eth_archive_core::types::{Block, Log, Transaction};
use eth_archive_parquet_writer::{Blocks, Logs, Transactions};
use parquet_writer::{ParquetConfig, ParquetWriter};
use std::path::PathBuf;
use tokio::sync::mpsc;

pub struct BlockData {
    pub block: Block,
    pub transactions: Vec<Transaction>,
    pub logs: Vec<Log>,
}

pub async fn generate_parquets(data: Vec<BlockData>, target_dir: &str) {
    let (tx, _rx) = mpsc::unbounded_channel();
    let block_range = BlockRange {
        from: usize::try_from(data.first().unwrap().block.number.0).unwrap(),
        to: usize::try_from(data.last().unwrap().block.number.0).unwrap(),
    };

    let mut blocks = vec![];
    let mut transactions = vec![];
    let mut logs = vec![];
    for mut item in data {
        blocks.push(item.block);
        transactions.append(&mut item.transactions);
        logs.append(&mut item.logs);
    }

    let block_config = ParquetConfig {
        name: "block".to_string(),
        path: PathBuf::from(target_dir).join("block"),
        items_per_file: blocks.len(),
        items_per_row_group: blocks.len(),
        channel_size: 1,
    };
    let block_writer = ParquetWriter::<Blocks>::new(block_config, tx.clone());

    let transaction_config = ParquetConfig {
        name: "tx".to_string(),
        path: PathBuf::from(target_dir).join("tx"),
        items_per_file: transactions.len(),
        items_per_row_group: transactions.len(),
        channel_size: 1,
    };
    let transaction_writer = ParquetWriter::<Transactions>::new(transaction_config, tx.clone());

    let log_config = ParquetConfig {
        name: "log".to_string(),
        path: PathBuf::from(target_dir).join("log"),
        items_per_file: logs.len(),
        items_per_row_group: logs.len(),
        channel_size: 1,
    };
    let log_writer = ParquetWriter::<Logs>::new(log_config, tx);

    block_writer.send((block_range, blocks)).await;
    transaction_writer.send((block_range, transactions)).await;
    log_writer.send((block_range, logs)).await;

    block_writer._join();
    transaction_writer._join();
    log_writer._join();
}
