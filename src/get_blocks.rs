use tokio::sync::mpsc;
use web3::types::BlockId;
use crate::types::BlockData;
use web3::Web3;
use web3::transports::WebSocket;

pub struct GetBlocks {
	failed: Vec<u64>,
	head: u64,
	client: Web3<WebSocket>,
}

pub type BlockFuture = Pin<Box<dyn Future<Output = Result<BlockData, Error>> + Send + Sync>>;

impl Iterator for GetBlocks {
    type Item = BlockFuture;

    fn next(&mut self) -> Option<Self::Item> {
        if self.block_num >= self.end {
            return None;
        }

        let end = cmp::min(self.end, self.block_num + self.batch_size);

        let client = self.client.clone();
        let block_num = self.block_num;

        let fut = async move {
            let blocks = client
                .send_batch(
                    (block_num..end)
                        .map(|block_number| GetBlockByNumber { block_number })
                        .collect(),
                )
                .await?;

            let mut txs = Vec::new();

            for block in blocks {
                txs.extend_from_slice(&block.transactions);
            }

            Ok(txs)
        };

        self.block_num += self.batch_size;

        Some(Box::pin(fut))
    }
}
