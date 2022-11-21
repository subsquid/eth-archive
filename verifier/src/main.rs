use eth_archive_verifier::{Config, Verifier};

#[tokio::main]
async fn main() {
    env_logger::init();

    let config = Config::parse();

    let verifier = match Verifier::new(config).await {
        Ok(verifier) => verifier,
        Err(e) => {
            log::error!("failed to create verifier:\n{}", e);
            return;
        }
    };

    if let Err(e) = verifier.run().await {
        log::error!("failed to run verifier:\n{}", e);
    }
}
