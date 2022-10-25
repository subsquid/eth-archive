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
pub struct DbConfig {
    /// Database user
    #[clap(long = "db-user")]
    pub user: String,

    /// Database user's password
    #[clap(long = "db-password")]
    pub password: String,

    /// Database name
    #[clap(long = "db-name")]
    pub dbname: String,

    /// Database host
    #[clap(long = "db-host")]
    pub host: String,

    /// Database port
    #[clap(long = "db-port")]
    pub port: u16,
}

#[derive(Parser, Clone, Debug)]
pub struct IngestConfig {
    /// An ethereum node rpc url
    #[clap(long, short)]
    pub eth_rpc_url: url::Url,

    /// An ethereum node request timeout
    #[clap(long)]
    pub request_timeout_secs: NonZeroU64,

    /// An ethereum node connection timeout
    #[clap(long)]
    pub connect_timeout_ms: NonZeroU64,

    /// Count of blocks to be downloaded by a batch concurrent request
    #[clap(long)]
    pub block_batch_size: usize,

    /// Number of concurrent requests to an rpc node
    #[clap(long)]
    pub http_req_concurrency: usize,
}
