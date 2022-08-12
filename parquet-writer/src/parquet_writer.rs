use crate::config::ParquetConfig;
use crate::schema::{BlockNum, IntoRowGroups};
use crate::{Error, Result};
use arrow2::io::parquet::write::*;
use eth_archive_core::types::BlockRange;
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

        let from_block = Self::get_start_block(&cfg.path).unwrap();

        let join_handle = std::thread::spawn(move || {
            let mut row_group = vec![T::default()];
            let mut block_range = None;

            let write_group = |row_group: &mut Vec<T>, block_range: &mut Option<BlockRange>| {
                let row_group = mem::take(row_group);
                let block_range = block_range.take().unwrap();
                let (row_groups, schema, options) = T::into_row_groups(row_group);

                let file_name = format!("{}{}_{}", &cfg.name, block_range.from, block_range.to);

                let start_time = Instant::now();

                let mut temp_path = cfg.path.clone();
                temp_path.push(format!("{}.temp", &file_name));
                let file = fs::File::create(&temp_path).unwrap();
                let mut writer = FileWriter::try_new(file, schema, options).unwrap();

                for group in row_groups {
                    writer.write(group.unwrap()).unwrap();
                }
                writer.end(None).unwrap();

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
                    if elem.block_num() >= from_block {
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

    fn get_start_block<P: AsRef<Path>>(path: P) -> Result<usize> {
        let dir = fs::read_dir(path).map_err(Error::ReadParquetDir)?;
        let mut block_num = 0;
        for entry in dir {
            let file_name = entry
                .unwrap()
                .file_name()
                .into_string()
                .map_err(|_| Error::InvalidParquetFileName)?;
            let num = file_name.split('_').last().unwrap();
            let num = &num[..num.len() - ".parquet".len()];
            let num = num.parse::<usize>().unwrap();
            block_num = cmp::max(block_num, num);
        }

        Ok(block_num)
    }
}
