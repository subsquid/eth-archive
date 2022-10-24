use crate::config::ParquetConfig;
use crate::schema::IntoRowGroups;
use crate::{Error, Result};
use arrow2::io::parquet::write::*;
use eth_archive_core::types::BlockRange;
use itertools::Itertools;
use std::path::Path;
use std::time::Instant;
use std::{cmp, fs, mem};
use tokio::sync::mpsc;

pub struct ParquetWriter<T: IntoRowGroups> {
    tx: Sender<T::Elem>,
    _join_handle: std::thread::JoinHandle<()>,
    pub cfg: ParquetConfig,
    pub from_block: usize,
}

type Sender<E> = mpsc::Sender<(BlockRange, Vec<E>)>;
type Receiver<E> = mpsc::Receiver<(BlockRange, Vec<E>)>;

impl<T: IntoRowGroups> ParquetWriter<T> {
    pub fn new(config: ParquetConfig, delete_tx: mpsc::UnboundedSender<usize>) -> Self {
        let cfg = config.clone();

        let (tx, mut rx): (Sender<T::Elem>, Receiver<T::Elem>) = mpsc::channel(config.channel_size);

        fs::create_dir_all(&cfg.path).unwrap();

        let from_block = Self::get_start_block(&cfg.path, &cfg.name).unwrap();

        let join_handle = std::thread::spawn(move || {
            let mut row_group = vec![T::default()];
            let mut block_range = None;

            let write_group = |row_group: &mut Vec<T>, block_range: &mut Option<BlockRange>| {
                let start_time = Instant::now();

                let row_group = mem::take(row_group);
                let block_range = block_range.take().unwrap();
                let (row_groups, schema, options) = T::into_row_groups(row_group);

                let file_name = format!("{}{}_{}", &cfg.name, block_range.from, block_range.to);

                log::info!(
                    "starting to write {}s {}-{} to parquet file",
                    &cfg.name,
                    block_range.from,
                    block_range.to,
                );

                let mut temp_path = cfg.path.clone();
                temp_path.push(format!("{}.temp", &file_name));
                let file = fs::File::create(&temp_path).unwrap();
                let mut writer = FileWriter::try_new(file, schema, options).unwrap();

                for group in row_groups {
                    writer.write(group.unwrap()).unwrap();
                }
                writer.end(None).unwrap();

                let file = writer.into_inner();

                file.sync_all().unwrap();
                mem::drop(file);

                let mut final_path = cfg.path.clone();
                final_path.push(format!("{}.parquet", &file_name));
                fs::rename(&temp_path, final_path).unwrap();

                log::info!(
                    "wrote {}s {}-{} to parquet file in {}ms",
                    &cfg.name,
                    block_range.from,
                    block_range.to,
                    start_time.elapsed().as_millis()
                );

                delete_tx.send(block_range.to).unwrap();
            };

            while let Some((other_range, elems)) = rx.blocking_recv() {
                let row = row_group.last_mut().unwrap();

                let row = if row.len() >= cfg.items_per_row_group {
                    if row_group.iter().map(IntoRowGroups::len).sum::<usize>() >= cfg.items_per_file
                    {
                        write_group(&mut row_group, &mut block_range);
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
                write_group(&mut row_group, &mut block_range);
            }
        });

        Self {
            tx,
            _join_handle: join_handle,
            cfg: config,
            from_block,
        }
    }

    pub async fn send(&self, msg: (BlockRange, Vec<T::Elem>)) {
        if self.tx.send(msg).await.is_err() {
            panic!("failed to send msg to parquet writer");
        }
    }

    pub fn _join(self) {
        mem::drop(self.tx);
        self._join_handle.join().unwrap();
    }

    fn get_start_block<P: AsRef<Path>>(path: P, prefix: &str) -> Result<usize> {
        let dir = fs::read_dir(&path).map_err(Error::ReadParquetDir)?;

        let mut ranges = Vec::new();
        let mut next_block_num = 0;

        for entry in dir {
            let entry = entry.unwrap();

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

        ranges.sort_by_key(|r| r.start);

        for ((_, r1), (i, r2)) in ranges.iter().enumerate().tuple_windows() {
            // check for a gap
            if r1.end != r2.start {
                // delete files that come after the gap
                for range in ranges[i..].iter() {
                    let mut path = path.as_ref().to_path_buf();
                    path.push(format!("{}{}_{}.parquet", prefix, range.start, range.end));

                    fs::remove_file(&path).unwrap();
                }
                return Ok(r1.end);
            }
        }

        Ok(next_block_num)
    }
}
