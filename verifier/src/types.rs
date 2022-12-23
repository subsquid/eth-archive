use eth_archive_worker::BlockEntryVec;
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArchiveResponse {
    pub data: Vec<BlockEntryVec>,
    pub archive_height: u32,
    pub next_block: u32,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Height {
    pub height: Option<u32>,
}
