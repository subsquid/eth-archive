use eth_archive_worker::{Config, Server};

use jemallocator::Jemalloc;

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
