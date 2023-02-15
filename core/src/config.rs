use clap::Parser;
use std::num::{NonZeroU64, NonZeroUsize};

#[derive(Parser, Clone, Copy, Debug)]
pub struct RetryConfig {
    #[clap(long)]
    pub num_tries: Option<usize>,
    #[clap(long, default_value_t = 3)]
    pub secs_between_tries: u64,
}

#[derive(Parser, Clone, Debug)]
pub struct IngestConfig {
    /// Http request timeout in seconds
    #[clap(long)]
    pub request_timeout_secs: NonZeroU64,
    /// Http connect timeout in milliseconds
    #[clap(long)]
    pub connect_timeout_ms: NonZeroU64,
    /// Number of blocks a single get block batch request will cover
    #[clap(long)]
    pub block_batch_size: u32,
    /// Number of concurrent requests to make
    #[clap(long)]
    pub http_req_concurrency: u32,
    /// Offset from tip of the chain (to avoid rollbacks)
    #[clap(long)]
    pub best_block_offset: u32,
    #[clap(long)]
    pub rpc_urls: Vec<url::Url>,
    /// Get transaction receipts. This fills the transaction.status field.
    /// Requires eth_getBlockReceipts to be available on the RPC API.
    #[clap(long, default_value_t = false)]
    pub get_receipts: bool,
    /// Wait this amount of seconds between rounds
    #[clap(long)]
    pub wait_between_rounds: Option<u64>,
}

#[derive(Parser, Clone, Debug)]
pub struct S3Config {
    #[clap(long)]
    pub s3_endpoint: Option<String>,
    #[clap(long)]
    pub s3_bucket_name: Option<String>,
    #[clap(long)]
    pub s3_bucket_region: Option<String>,
    #[clap(long)]
    pub s3_sync_interval_secs: Option<u64>,
    #[clap(long)]
    pub s3_concurrency: Option<NonZeroUsize>,
}

#[derive(Clone)]
pub struct ParsedS3Config {
    pub s3_endpoint: String,
    pub s3_bucket_name: String,
    pub s3_bucket_region: String,
    pub s3_sync_interval_secs: u64,
    pub s3_concurrency: NonZeroUsize,
}

impl S3Config {
    pub fn into_parsed(&self) -> Option<ParsedS3Config> {
        match (
            self.s3_endpoint.clone(),
            self.s3_bucket_name.clone(),
            self.s3_bucket_region.clone(),
            self.s3_sync_interval_secs,
        ) {
            (
                Some(s3_endpoint),
                Some(s3_bucket_name),
                Some(s3_bucket_region),
                Some(s3_sync_interval_secs),
            ) => Some(ParsedS3Config {
                s3_endpoint,
                s3_bucket_name,
                s3_bucket_region,
                s3_sync_interval_secs,
                s3_concurrency: self.s3_concurrency.unwrap_or(NonZeroUsize::new(6).unwrap()),
            }),
            _ => None,
        }
    }
}
