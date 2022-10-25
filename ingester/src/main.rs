use eth_archive_ingester::{Config, Ingester};

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[tokio::main]
async fn main() {
    env_logger::init();

    let config = Config::parse();

    let ingester = match Ingester::new(config).await {
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
