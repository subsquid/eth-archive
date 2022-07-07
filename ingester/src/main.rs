use clap::Parser;
use eth_archive_ingester::{Ingester, Options};
use std::process;

#[tokio::main]
async fn main() {
    env_logger::init();

    let options = Options::parse();

    let ingester = match Ingester::new(&options).await {
        Ok(ingester) => ingester,
        Err(e) => {
            log::error!("failed to create ingester:\n{}", e);
            process::abort();
        }
    };

    if let Err(e) = ingester.run().await {
        log::error!("failed to run ingester:\n{}", e);
        process::abort();
    }
}
