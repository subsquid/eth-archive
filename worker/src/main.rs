use eth_archive_worker::{Config, Server};

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[tokio::main]
async fn main() {
    env_logger::init();

    let config = Config::parse();

    if let Err(e) = Server::run(config).await {
        log::error!("failed to run server:\n{}", e);
    }
}
