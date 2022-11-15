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
    #[clap(long, default_value_t = 10)]
    pub best_block_offset: u32,
    #[clap(long)]
    pub default_rpc_url: Option<String>,
}

#[derive(Parser, Clone, Debug)]
pub struct S3Config {
    #[clap(long)]
    pub s3_endpoint: Option<String>,
    #[clap(long)]
    pub s3_bucket_name: Option<String>,
    #[clap(long)]
    pub s3_bucket_region: Option<String>,
}

pub struct ParsedS3Config {
    pub s3_endpoint: String,
    pub s3_bucket_name: String,
    pub s3_bucket_region: String,
}

impl S3Config {
    pub fn into_parsed(&self) -> Option<ParsedS3Config> {
        match (
            self.s3_endpoint.clone(),
            self.s3_bucket_name.clone(),
            self.s3_bucket_region.clone(),
        ) {
            (Some(s3_endpoint), Some(s3_bucket_name), Some(s3_bucket_region)) => {
                Some(ParsedS3Config {
                    s3_endpoint,
                    s3_bucket_name,
                    s3_bucket_region,
                })
            }
            _ => None,
        }
    }
}
