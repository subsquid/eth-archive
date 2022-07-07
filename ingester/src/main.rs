use eth_archive_ingester::Ingester;

#[tokio::main]
async fn main() {
    let _ingester = match Ingester::from_cfg_path("EthIngester.toml").await {
        Ok(ingester) => ingester,
        Err(e) => {
            eprintln!("failed to create ingester:\n{}", e);
            return;
        }
    };
}
