use crate::config::Config;
use crate::{Error, Result};
use eth_archive_core::eth_client::EthClient;
use crate::archive_client::ArchiveClient;

pub struct Verifier {
	eth_client: Arc<EthClient>,
	archive_client: Arc<ArchiveClient>,
}

impl Verifier {
    pub async fn new(config: Config) -> Result<Self> {
        todo!()
    }

    pub async fn run(self) -> Result<()> {
        todo!()
    }
}
