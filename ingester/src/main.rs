use clap::Parser;
use eth_archive_ingester::{Ingester, Options};

#[tokio::main]
async fn main() {
    env_logger::init();

    let options = Options::parse();

    let _ingester = match Ingester::from_cfg_path(&options, "EthIngester.toml").await {
        Ok(ingester) => ingester,
        Err(e) => {
            log::error!("failed to create ingester:\n{}", e);
            return;
        }
    };
}
