use crate::Result;
use eth_archive_core::config::ParsedS3Config;
use eth_archive_core::types::{Block, BlockRange, Log};
use futures::Stream;
use std::pin::Pin;

mod ver0_0_39;

type Data = Pin<Box<dyn Stream<Item = Result<(Vec<BlockRange>, Vec<Vec<Block>>, Vec<Vec<Log>>)>>>>;

pub async fn stream_batches(
    config: &ParsedS3Config,
    s3_src_bucket: &str,
    s3_src_format_ver: &str,
) -> Result<(Data, u32)> {
    todo!();
}
