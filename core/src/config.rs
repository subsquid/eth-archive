use clap::Parser;
use std::num::NonZeroU64;

#[derive(Parser, Clone, Copy, Debug)]
pub struct RetryConfig {
    #[clap(long)]
    pub num_tries: Option<usize>,
    #[clap(long, default_value_t = 3)]
    pub secs_between_tries: u64,
}

#[derive(Parser, Clone, Debug)]
pub struct IngestConfig {
    /// Ethereum node RPC url
    #[clap(long, short)]
    pub eth_rpc_url: url::Url,
    /// Http request timeout in seconds
    #[clap(long)]
    pub request_timeout_secs: NonZeroU64,
    /// Http connect timeout in milliseconds
    #[clap(long)]
    pub connect_timeout_ms: NonZeroU64,
    /// Number of blocks a single batch request will cover
    #[clap(long)]
    pub block_batch_size: u32,
    /// Number of concurrent requests to make
    #[clap(long)]
    pub http_req_concurrency: u32,
    /// Offset from tip of the chain (to avoid rollbacks)
    #[clap(long)]
    pub best_block_offset: u32,
}
