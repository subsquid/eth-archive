use eth_archive_gateway::{Options, Server};

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[tokio::main]
async fn main() {
    env_logger::init();

    let options = Options::parse();

    if let Err(e) = Server::run(&options).await {
        log::error!("failed to run server:\n{}", e);
    }
}
