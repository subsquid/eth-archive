use arrow2::array::*;
use arrow2::chunk::Chunk;
use arrow2::datatypes::{DataType, Field, Schema};
use arrow2::io::parquet::write::*;
use datafusion::execution::context::ExecutionContext;
use std::fs;
use std::sync::Arc;

#[tokio::main]
async fn main() {
    let block_schema = Schema::from(vec![
        Field::new("number", DataType::UInt64, false),
        Field::new("timestamp", DataType::UInt64, false),
        Field::new("nonce", DataType::Utf8, false),
        Field::new("size", DataType::Utf8, false),
    ]);

    let tx_schema = Schema::from(vec![
        Field::new("hash", DataType::Utf8, false),
        Field::new("nonce", DataType::Utf8, false),
        Field::new("block_hash", DataType::Utf8, true),
        Field::new("block_number", DataType::UInt64, true),
        Field::new("transaction_index", DataType::Utf8, true),
        Field::new("from", DataType::Utf8, false),
        Field::new("to", DataType::Utf8, true),
        Field::new("value", DataType::Utf8, false),
        Field::new("gas_price", DataType::Utf8, false),
        Field::new("gas", DataType::Utf8, false),
        Field::new("input", DataType::Utf8, false),
        Field::new("public_key", DataType::Utf8, false),
        Field::new("chain_id", DataType::Utf8, true),
    ]);

    let log_schema = Schema::from(vec![
        Field::new("removed", DataType::Boolean, false),
        Field::new("log_index", DataType::Utf8, false),
        Field::new("transaction_index", DataType::Utf8, true),
        Field::new("transaction_hash", DataType::Utf8, true),
        Field::new("block_hash", DataType::Utf8, true),
        Field::new("block_number", DataType::UInt64, true),
        Field::new("address", DataType::Utf8, false),
        Field::new("data", DataType::Utf8, false),
        Field::new(
            "topics",
            DataType::List(Box::new(Field::new("topic", DataType::Utf8, false))),
            false,
        ),
    ]);

    let mut topics: MutableListArray<i64, MutableUtf8Array<i32>> = MutableListArray::new();
    topics
        .try_push(Some(
            Utf8Array::<i32>::from(&[Some("one".to_owned()), Some("two".to_owned())]).iter(),
        ))
        .unwrap();

    topics
        .try_push(Some(
            Utf8Array::<i32>::from(&[Some("three".to_owned())]).iter(),
        ))
        .unwrap();

    let chunk = Chunk::new(vec![
        Arc::new(UInt64Array::from(&[Some(3), Some(5)])) as Arc<dyn Array>,
        topics.into_arc(),
    ]);

    let options = WriteOptions {
        write_statistics: true,
        compression: CompressionOptions::Snappy,
        version: Version::V1,
    };

    let row_groups = RowGroupIterator::try_new(
        vec![Ok(chunk)].into_iter(),
        &log_schema,
        options,
        vec![Encoding::Plain, Encoding::Plain],
    )
    .unwrap();

    let file = fs::File::create("anan.parquet").unwrap();
    let mut writer = FileWriter::try_new(file, log_schema, options).unwrap();

    writer.start().unwrap();
    for group in row_groups {
        writer.write(group.unwrap()).unwrap();
    }
    writer.end(None).unwrap();

    let mut ctx = ExecutionContext::new();
    ctx.register_parquet("log", "file://anan.parquet")
        .await
        .unwrap();
    let df = ctx.sql("SELECT * FROM log;").await.unwrap();

    df.show().await.unwrap();
}
