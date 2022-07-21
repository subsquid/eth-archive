use clap::Parser;
use eth_archive_parquet_writer::{Options, ParquetWriterRunner};

#[tokio::main]
async fn main() {
    env_logger::init();

    let options = Options::parse();

    let runner = match ParquetWriterRunner::new(&options).await {
        Ok(runner) => runner,
        Err(e) => {
            log::error!("failed to create parquet writer runner:\n{}", e);
            return;
        }
    };

    if let Err(e) = runner.run().await {
        log::error!("failed to run parquet writer runner:\n{}", e);
    }
}
