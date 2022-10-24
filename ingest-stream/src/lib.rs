use eth_archive_core::config::IngestConfig;
use eth_archive_core::eth_client::EthClient;
use eth_archive_core::eth_request::{GetBlockByNumber, GetLogs};
use eth_archive_core::retry::Retry;
use eth_archive_core::types::BlockRange;
use eth_archive_core::types::{Block, Log};
use futures::stream::Stream;
use std::cmp;
use std::sync::Arc;
use std::time::Instant;

pub struct IngestClient {
    eth_client: Arc<EthClient>,
    retry: Retry,
    cfg: IngestConfig,
}

impl IngestClient {
    pub fn new(cfg: IngestConfig, eth_client: Arc<EthClient>, retry: Retry) -> Self {
        Self {
            cfg,
            eth_client,
            retry,
        }
    }

    pub fn ingest_batches(
        self: Arc<Self>,
        from_block: u32,
        to_block: u32,
    ) -> impl Stream<Item = eth_archive_core::Result<(Vec<BlockRange>, Vec<Vec<Block>>, Vec<Vec<Log>>)>>
    {
        assert!(to_block > from_block);
        let from_block = usize::try_from(from_block).unwrap();
        let to_block = usize::try_from(to_block).unwrap();

        async_stream::try_stream! {
            let step = self.cfg.http_req_concurrency * self.cfg.block_batch_size;
            for block_num in (from_block..to_block).step_by(step) {
                let concurrency = self.cfg.http_req_concurrency;
                let batch_size = self.cfg.block_batch_size;

                let block_batches = (0..concurrency)
                    .filter_map(|step_no| {
                        let start = block_num + step_no * batch_size;
                        let end = cmp::min(start + batch_size, to_block);

                        let batch = (start..end)
                            .map(|i| GetBlockByNumber { block_number: i })
                            .collect::<Vec<_>>();

                        if batch.is_empty() {
                            None
                        } else {
                            Some(batch)
                        }
                    })
                    .collect::<Vec<_>>();

                let log_batches = (0..concurrency)
                    .filter_map(|step_no| {
                        let start = block_num + step_no * batch_size;
                        let end = cmp::min(start + batch_size, to_block);

                        if start < end {
                            Some(GetLogs {
                                from_block: start,
                                to_block: end - 1,
                            })
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>();

                let start_time = Instant::now();

                log::info!(
                    "starting to download blocks {}-{}",
                    block_num,
                    cmp::min(block_num + step, to_block),
                );

                let block_batches = self
                    .eth_client
                    .clone()
                    .send_batches(&block_batches, self.retry)
                    .await?;
                let log_batches = self
                    .eth_client
                    .clone()
                    .send_concurrent(&log_batches, self.retry)
                    .await?;

                log::info!(
                    "downloaded blocks {}-{} in {}ms",
                    block_num,
                    cmp::min(block_num + step, to_block),
                    start_time.elapsed().as_millis()
                );

                let block_ranges = (0..block_batches.len()).map(|i| {
                    let start = block_num + i * batch_size;
                    let end = cmp::min(start + batch_size, to_block);
                    let block_range = BlockRange {
                        from: start,
                        to: end,
                    };
                    block_range
                }).collect();

                yield (
                    block_ranges,
                    block_batches,
                    log_batches,
                );
            }
        }
    }
}
