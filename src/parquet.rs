use crate::error::Error;
use crate::eth_client::TxsInRange;
use crate::eth_request::Transaction;
use parquet::file::properties::WriterProperties;
use parquet::file::writer::FileWriter;
use parquet::file::writer::SerializedFileWriter;
use parquet::record::RecordWriter;
use std::collections::VecDeque;
use std::sync::Arc;

pub async fn write_txs_in_range(txs_in_range: TxsInRange, path: &str) -> WriteTxsInRangeResult {
    let mut errors = Vec::new();

    let file = std::fs::File::create(&path).unwrap();

    let props = Arc::new(WriterProperties::builder().build());

    let schema = [Transaction::default()].as_slice().schema().unwrap();

    let mut writer = SerializedFileWriter::new(file, schema, props).unwrap();

    let mut txs_in_range = txs_in_range.collect::<VecDeque<_>>();

    const REQ_BATCH_SIZE: usize = 16;

    for _ in 0..(txs_in_range.len() / REQ_BATCH_SIZE) {
        println!("running req batch");

        let mut handles = Vec::with_capacity(REQ_BATCH_SIZE);
        for _ in 0..REQ_BATCH_SIZE {
            let batch = txs_in_range.pop_front().unwrap();
            handles.push(tokio::spawn(batch));
        }
        for handle in handles {
            let batch = match handle.await.unwrap() {
                Ok(batch) => batch,
                Err(e) => {
                    errors.push(Error::GetTxBatch(Box::new(e)));
                    continue;
                }
            };

            let mut row_group = writer.next_row_group().unwrap();
            batch.as_slice().write_to_row_group(&mut row_group).unwrap();
            writer.close_row_group(row_group).unwrap();
        }

        println!("finished req batch");
    }

    for _ in 0..(txs_in_range.len() % REQ_BATCH_SIZE) {
        println!("running remainder batch");

        let batch = txs_in_range.pop_front().unwrap();

        let batch = match batch.await {
            Ok(batch) => batch,
            Err(e) => {
                errors.push(Error::GetTxBatch(Box::new(e)));
                continue;
            }
        };

        let mut row_group = writer.next_row_group().unwrap();
        batch.as_slice().write_to_row_group(&mut row_group).unwrap();
        writer.close_row_group(row_group).unwrap();

        println!("finished remainder batch");
    }

    writer.close().unwrap();

    WriteTxsInRangeResult { errors }
}

pub struct WriteTxsInRangeResult {
    pub errors: Vec<Error>,
}
