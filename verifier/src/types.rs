use eth_archive_worker::BlockEntryVec;
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArchiveResponse {
    data: Vec<BlockEntryVec>,
    archive_height: u32,
    next_block: u32,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Height {
    height: Option<u32>,
}
