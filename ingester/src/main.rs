use eth_archive_core::options::Options;
use eth_archive_ingester::Ingester;

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[tokio::main]
async fn main() {
    env_logger::init();

    let options = Options::parse();

    let ingester = match Ingester::new(&options).await {
        Ok(ingester) => ingester,
        Err(e) => {
            log::error!("failed to create ingester:\n{}", e);
            return;
        }
    };

    if let Err(e) = ingester.run().await {
        log::error!("failed to run ingester:\n{}", e);
    }
}
