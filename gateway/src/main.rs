use eth_archive_gateway::{Options, Server};

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[tokio::main]
async fn main() {
    env_logger::init();

    let options = Options::parse();

    if let Err(e) = Server::run(options).await {
        log::error!("failed to run server:\n{}", e);
    }
}
