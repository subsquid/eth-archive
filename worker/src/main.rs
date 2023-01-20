#![deny(warnings)]

use eth_archive_worker::{Config, Server};

use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[tokio::main]
async fn main() {
    env_logger::init();

    let config = Config::parse();

    if let Err(e) = Server::run(config).await {
        log::error!("failed to run server:\n{}", e);
    }
}
