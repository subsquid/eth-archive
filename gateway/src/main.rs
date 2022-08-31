use eth_archive_gateway::{Options, Server};

use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[tokio::main]
async fn main() {
    env_logger::init();

    let options = Options::parse();

    if let Err(e) = Server::run(&options).await {
        log::error!("failed to run server:\n{}", e);
    }
}
