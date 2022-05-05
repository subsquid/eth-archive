use std::sync::Arc;
use tracing::info;

#[tokio::main]
async fn main() {
    let transport = web3::transports::Http::new(
        "https://eth-mainnet.alchemyapi.io/v2/DPijdCeN4cXDxSQT8eFfGETr2xhMegT0",
    )
    .unwrap();
    let transport = web3::transports::Batch::new(transport);
    let client = web3::Web3::new(transport);
    let client = Arc::new(client);

    tracing::subscriber::set_global_default(tracing_subscriber::fmt::Subscriber::new()).unwrap();

    const BATCH_SIZE: usize = 100;
    const STEP: usize = 16;

    let start = std::time::Instant::now();
    let block_start = 12_000_000;
    let block_end = 14_000_000;

    for block_num in (block_start..block_end).step_by(BATCH_SIZE * STEP) {
        let futs = (0..STEP).map(|step_idx| {
            let client = client.clone();
            async move {
                let block_futs = (0..BATCH_SIZE)
                    .map(|batch_idx| {
                        let block_num = step_idx * BATCH_SIZE + batch_idx;
                        client
                            .clone()
                            .eth()
                            .block_with_txs(web3::types::U64::from(block_num).into())
                    })
                    .collect::<Vec<_>>();

                client.transport().submit_batch().await.unwrap();

                block_futs.into_iter()
            }
        });

        let batch = futures::future::join_all(futs).await.into_iter().flatten();

        for block in batch {
        	let block = block.await.unwrap();

        	info!("{:?}", block);
        }

        let blocks_processed = block_num - block_start;

        let blocks_ps = blocks_processed as u64 / start.elapsed().as_secs();

        info!("head={};{}blocks/s", block_num, blocks_ps);
    }
}
