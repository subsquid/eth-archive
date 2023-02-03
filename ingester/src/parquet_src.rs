use crate::{Error, Result};
use aws_config::retry::RetryConfig;
use eth_archive_core::config::ParsedS3Config;
use eth_archive_core::ingest_metrics::IngestMetrics;
use eth_archive_core::types::{Block, BlockRange, Log};
use futures::Stream;
use std::pin::Pin;
use std::sync::Arc;

mod ver0_0_39;

type Data = Pin<Box<dyn Stream<Item = Result<(Vec<BlockRange>, Vec<Vec<Block>>, Vec<Vec<Log>>)>>>>;

pub async fn stream_batches(
    ingest_metrics: Arc<IngestMetrics>,
    start_block: u32,
    config: &ParsedS3Config,
    s3_src_bucket: &str,
    s3_src_format_ver: &str,
) -> Result<Data> {
    let cfg = aws_config::from_env()
        .retry_config(RetryConfig::standard())
        .endpoint_url(&config.s3_endpoint)
        .region(aws_types::region::Region::new(
            config.s3_bucket_region.clone(),
        ))
        .load()
        .await;
    let client = aws_sdk_s3::Client::new(&cfg);
    let client = Arc::new(client);

    match s3_src_format_ver {
        "0.0.39" => {
            ver0_0_39::execute(ingest_metrics, start_block, s3_src_bucket.into(), client).await
        }
        _ => Err(Error::UnknownFormatVersion(s3_src_format_ver.to_owned())),
    }
}
