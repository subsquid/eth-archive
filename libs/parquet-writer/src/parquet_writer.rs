use crate::config::ParquetConfig;
use crate::schema::IntoRowGroups;
use crate::{Error, Result};
use arrow2::io::parquet::write::*;
use eth_archive_core::types::BlockRange;
use futures::SinkExt;
use itertools::Itertools;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;
use std::{cmp, mem};
use tokio::sync::mpsc;
use tokio_util::compat::TokioAsyncReadCompatExt;

pub struct ParquetWriter<T: IntoRowGroups> {
    tx: Sender<T::Elem>,
    join_handle: tokio::task::JoinHandle<()>,
    pub cfg: ParquetConfig,
    pub from_block: usize,
}

type Sender<E> = mpsc::Sender<(BlockRange, Vec<E>)>;
type Receiver<E> = mpsc::Receiver<(BlockRange, Vec<E>)>;

impl<T: IntoRowGroups + 'static + Send + Sync> ParquetWriter<T> {
    pub async fn new(config: ParquetConfig, delete_tx: mpsc::UnboundedSender<usize>) -> Self {
        let cfg = Arc::new(config.clone());

        let (tx, mut rx): (Sender<T::Elem>, Receiver<T::Elem>) = mpsc::channel(cfg.channel_size);

        tokio::fs::create_dir_all(&cfg.path).await.unwrap();

        let from_block = Self::get_start_block(&cfg.path, &cfg.name).await.unwrap();

        let join_handle = tokio::spawn(async move {
            let mut row_group = vec![T::default()];
            let mut block_range: Option<BlockRange> = None;

            while let Some((other_range, elems)) = rx.recv().await {
                let row = row_group.last_mut().unwrap();

                let row = if row.len() >= cfg.items_per_row_group {
                    if row_group.iter().map(IntoRowGroups::len).sum::<usize>() >= cfg.items_per_file
                    {
                        let row_group = mem::take(&mut row_group);
                        let block_range = block_range.take().unwrap();
                        Self::write_group(row_group, block_range, cfg.clone(), delete_tx.clone())
                            .await;
                    }
                    row_group.push(T::default());
                    row_group.last_mut().unwrap()
                } else {
                    row
                };

                block_range = match block_range {
                    Some(block_range) => Some(block_range.merge(other_range)),
                    None => Some(other_range),
                };

                if let Some(block_range) = block_range.as_mut() {
                    block_range.from = cmp::max(block_range.from, from_block)
                }

                for elem in elems.into_iter() {
                    let block_num: usize = row.block_num(&elem).try_into().unwrap();
                    if block_num >= from_block {
                        row.push(elem).unwrap();
                    }
                }
            }

            if row_group.last_mut().unwrap().len() > 0 {
                let row_group = mem::take(&mut row_group);
                let block_range = block_range.take().unwrap();
                Self::write_group(row_group, block_range, cfg.clone(), delete_tx.clone()).await;
            }
        });

        Self {
            tx,
            join_handle,
            cfg: config,
            from_block,
        }
    }

    pub async fn send(&self, msg: (BlockRange, Vec<T::Elem>)) {
        if self.tx.send(msg).await.is_err() {
            panic!("failed to send msg to parquet writer");
        }
    }

    pub async fn join(self) {
        mem::drop(self.tx);
        self.join_handle.await.unwrap();
    }

    async fn write_group(
        row_group: Vec<T>,
        block_range: BlockRange,
        cfg: Arc<ParquetConfig>,
        delete_tx: mpsc::UnboundedSender<usize>,
    ) {
        let start_time = Instant::now();

        let (chunks, schema, encodings, options) =
            rayon_async::spawn(move || T::into_row_groups(row_group)).await;

        let file_name = format!("{}{}_{}", &cfg.name, block_range.from, block_range.to);

        log::info!(
            "starting to write {}s {}-{} to parquet file",
            &cfg.name,
            block_range.from,
            block_range.to,
        );

        let mut temp_path = cfg.path.clone();
        temp_path.push(format!("{}.temp", &file_name));
        let file = tokio::fs::File::create(&temp_path).await.unwrap().compat();

        let mut writer = FileSink::try_new(file, schema, encodings, options).unwrap();

        let mut chunk_stream = futures::stream::iter(chunks);
        writer.send_all(&mut chunk_stream).await.unwrap();
        writer.close().await.unwrap();

        /*
        let file = writer.into_inner();
        file.sync_all().await.unwrap();
        mem::drop(file);
        */

        let mut final_path = cfg.path.clone();
        final_path.push(format!("{}.parquet", &file_name));
        tokio::fs::rename(&temp_path, final_path).await.unwrap();

        log::info!(
            "wrote {}s {}-{} to parquet file in {}ms",
            &cfg.name,
            block_range.from,
            block_range.to,
            start_time.elapsed().as_millis()
        );

        delete_tx.send(block_range.to).unwrap();
    }

    async fn get_start_block<P: AsRef<Path>>(path: P, prefix: &str) -> Result<usize> {
        let mut dir = tokio::fs::read_dir(&path)
            .await
            .map_err(Error::ReadParquetDir)?;

        let mut ranges = Vec::new();
        let mut next_block_num = 0;

        while let Some(entry) = dir.next_entry().await.unwrap() {
            let file_name = entry
                .file_name()
                .into_string()
                .map_err(|_| Error::ReadParquetFileName)?;
            let file_name = match file_name.strip_prefix(prefix) {
                Some(file_name) => file_name,
                None => return Err(Error::InvalidParquetFilename(file_name.to_owned())),
            };
            let file_name = match file_name.strip_suffix(".parquet") {
                Some(file_name) => file_name,
                None => continue,
            };
            let (start, end) = file_name
                .split_once('_')
                .ok_or_else(|| Error::InvalidParquetFilename(file_name.to_owned()))?;

            let (start, end) = {
                let start = start
                    .parse::<usize>()
                    .map_err(|_| Error::InvalidParquetFilename(file_name.to_owned()))?;
                let end = end
                    .parse::<usize>()
                    .map_err(|_| Error::InvalidParquetFilename(file_name.to_owned()))?;

                (start, end)
            };

            ranges.push(start..end);
            next_block_num = cmp::max(next_block_num, end);
        }

        let ranges = rayon_async::spawn(move || {
            ranges.sort_by_key(|r| r.start);
            ranges
        })
        .await;

        for ((_, r1), (i, r2)) in ranges.iter().enumerate().tuple_windows() {
            // check for a gap
            if r1.end != r2.start {
                // delete files that come after the gap
                for range in ranges[i..].iter() {
                    let mut path = path.as_ref().to_path_buf();
                    path.push(format!("{}{}_{}.parquet", prefix, range.start, range.end));

                    tokio::fs::remove_file(&path).await.unwrap();
                }
                return Ok(r1.end);
            }
        }

        Ok(next_block_num)
    }
}
