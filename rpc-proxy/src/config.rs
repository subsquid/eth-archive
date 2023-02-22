use clap::Parser;
use eth_archive_core::config::RetryConfig;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::num::NonZeroU64;

#[derive(Clone, Parser, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct Config {
    #[command(flatten)]
    pub retry: RetryConfig,
    /// Address to be used for running server
    #[clap(long, default_value_t = default_server_addr())]
    pub server_addr: SocketAddr,
    /// Default rpc address to use
    #[clap(long)]
    pub default_target_rpc: Option<url::Url>,
    /// Treat batched requests as separate requests.
    /// This is useful if the target rpc counts individual requests inside a batch
    /// as separate requests
    #[clap(long)]
    pub separate_batches: bool,
    /// Maximum requests per second when sending requests to target rpc
    #[clap(long)]
    pub max_requests_per_sec: Option<usize>,
    /// Maximum block range on a eth_getLogs request
    #[clap(long)]
    pub max_get_logs_block_range: Option<u32>,
    /// Maximum rpc request batch size
    #[clap(long)]
    pub max_batch_size: Option<usize>,
    /// Http request timeout in seconds
    #[clap(long)]
    pub request_timeout_secs: NonZeroU64,
    /// Http connect timeout in milliseconds
    #[clap(long)]
    pub connect_timeout_ms: NonZeroU64,
}

fn default_server_addr() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8282)
}

impl Config {
    pub fn parse() -> Self {
        <Self as Parser>::parse()
    }
}
