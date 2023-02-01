use super::Data;
use crate::{Error, Result};
use eth_archive_core::s3_sync::get_list;

pub async fn execute(
    start_block: u32,
    s3_src_bucket: &str,
    client: &aws_sdk_s3::Client,
) -> Result<(Data, u32)> {
    let list = get_list(s3_src_bucket, client)
        .await
        .map_err(Error::ListS3BucketContents)?;

    todo!()
}
