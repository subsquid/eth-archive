use crate::dir_name::DirName;
use crate::ingest_metrics::IngestMetrics;
use crate::parquet_source::{self, read_parquet_buf, ParquetSource};
use crate::s3_client::BatchStream;
use crate::types::{Block, BlockRange, FormatVersion, Log};
use crate::{Error, Result};
use futures::{Stream, TryFutureExt};
use polars::export::arrow::datatypes::Field;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;

pub async fn stream_batches(
    ingest_metrics: Arc<IngestMetrics>,
    start_block: u32,
    local_source_path: &Path,
    local_format_version: &str,
) -> Result<BatchStream> {
    let source = parquet_source::get(FormatVersion::from_str(local_format_version)?);
    let dir_names = DirName::find_sorted(local_source_path, start_block).await?;

    Ok(Box::pin(stream_batches_impl(
        ingest_metrics,
        local_source_path,
        source,
        dir_names,
    )))
}

fn stream_batches_impl(
    ingest_metrics: Arc<IngestMetrics>,
    local_source_path: &Path,
    source: Box<dyn ParquetSource>,
    dir_names: Vec<DirName>,
) -> impl Stream<Item = Result<(Vec<BlockRange>, Vec<Vec<Block>>, Vec<Vec<Log>>)>> {
    let num_files = dir_names.len();

    log::info!("stream_batches: {} directories to stream.", num_files);

    let local_source_path = local_source_path.to_owned();

    let mut start_time = Instant::now();
    async_stream::try_stream! {
        for (i, dir_name) in dir_names.into_iter().enumerate() {
            let start = Instant::now();

            let read_fut = |kind, fields: Vec<Field>| {
                let mut path = local_source_path.clone();
                path.push(&dir_name.to_string());
                path.push(&format!("{kind}.parquet"));

                async move {
                    let file = tokio::fs::read(&path).await.map_err(Error::ReadFile)?;
                    read_parquet_buf(&file, fields)
                }
            };

            let block_fut = read_fut("block", source.block_fields()).map_ok(|columns| source.read_blocks(columns));
            let tx_fut = read_fut("tx", source.tx_fields()).map_ok(|columns| source.read_txs(columns));
            let log_fut = read_fut("log", source.log_fields()).map_ok(|columns| source.read_logs(columns));

            let (mut blocks, txs, logs) = futures::future::try_join3(block_fut, tx_fut, log_fut).await?;

            let block_range = BlockRange {
                from: *blocks.first_key_value().unwrap().0,
                to: blocks.last_key_value().unwrap().0 + 1,
            };

            for tx in txs {
                blocks.get_mut(&tx.block_number.0).unwrap().transactions.push(tx);
            }

            let blocks = blocks.into_values().collect::<Vec<_>>();

            let block_num = dir_name.range.to;

            if block_num > 0 {
                ingest_metrics.record_download_height(block_num-1);
            }
            let elapsed = start.elapsed().as_millis();
            if elapsed > 0 {
                ingest_metrics.record_download_speed((block_num-dir_name.range.from) as f64 / elapsed as f64 * 1000.);
            }

            if start_time.elapsed().as_secs() > 15 {
                let percentage = (i + 1) as f64 / num_files as f64 * 100.;
                log::info!(
                    "stream_batches progress: {}/{} {:.2}%",
                    i + 1,
                    num_files,
                    percentage
                );
                start_time = Instant::now();
            }

            yield (vec![block_range], vec![blocks], vec![logs]);
        }
    }
}
