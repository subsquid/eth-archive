use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::ListingOptions;
use datafusion::execution::context::ExecutionContext;
use eth_archive::EthClient;
use std::sync::Arc;

#[tokio::main]
async fn main() {
    //create_parquet_files(10_000_000, 12_000_000, 20_000).await;
    query_parquet_files().await;
}

async fn create_parquet_files(start_block: usize, end_block: usize, blocks_per_file: usize) {
    let client =
        EthClient::new("https://eth-mainnet.alchemyapi.io/v2/DPijdCeN4cXDxSQT8eFfGETr2xhMegT0")
            .unwrap();

    let client = Arc::new(client);

    const RPC_BATCH_SIZE: usize = 250;

    for block_num in (start_block..end_block).step_by(blocks_per_file) {
        let start = block_num;
        let end = std::cmp::min(end_block, block_num + blocks_per_file);

        println!("processing txs in range: {}-{}", start, end);

        let txs_in_range = client.clone().get_txs_in_range(start, end, RPC_BATCH_SIZE);

        let res = eth_archive::parquet::write_txs_in_range(
            txs_in_range,
            &format!("data/txs{}_{}.parquet", start, end),
        )
        .await;

        println!("saved txs in range: {}-{}", start, end);

        eprintln!("{:#?}", res.errors);
    }
}

async fn query_parquet_files() {
    let mut ctx = ExecutionContext::new();

    let file_format = ParquetFormat::default().with_enable_pruning(true);

    let listing_options = ListingOptions {
        file_extension: ".parquet".to_owned(),
        format: Arc::new(file_format),
        table_partition_cols: vec![],
        collect_stat: false,
        target_partitions: 1,
    };

    ctx.register_listing_table("txs", "file://./data/", listing_options, None)
        .await
        .unwrap();

    let data_frame = ctx
        .sql("SELECT COUNT(*) FROM txs WHERE to = '0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85';")
        .await
        .unwrap();

    data_frame.show().await.unwrap();
}
