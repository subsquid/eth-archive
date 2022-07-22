use crate::config::ParquetConfig;
use crate::schema::IntoRowGroups;
use arrow2::io::parquet::write::*;
use std::{fs, mem};
use tokio::sync::mpsc;

pub struct ParquetWriter<T: IntoRowGroups> {
    tx: mpsc::Sender<Vec<T::Elem>>,
    _join_handle: std::thread::JoinHandle<()>,
}

impl<T: IntoRowGroups> ParquetWriter<T> {
    pub fn new(cfg: ParquetConfig, delete_tx: mpsc::UnboundedSender<usize>) -> Self {
        let (tx, mut rx) = mpsc::channel(cfg.channel_size);

        fs::create_dir_all(&cfg.path).unwrap();

        let join_handle = std::thread::spawn(move || {
            let mut row_group = vec![T::default()];

            let write_group = |row_group: &mut Vec<T>| {
                let row_group = mem::take(row_group);
                let (row_groups, schema, options, block_range) = T::into_row_groups(row_group);

                let mut temp_path = cfg.path.clone();
                temp_path.push(format!(
                    "{}{}_{}.temp",
                    &cfg.name, block_range.from, block_range.to
                ));
                let file = fs::File::create(&temp_path).unwrap();
                let mut writer = FileWriter::try_new(file, schema, options).unwrap();

                writer.start().unwrap();
                for group in row_groups {
                    writer.write(group.unwrap()).unwrap();
                }
                writer.end(None).unwrap();

                let mut final_path = cfg.path.clone();
                final_path.push(format!(
                    "{}{}_{}.parquet",
                    &cfg.name, block_range.from, block_range.to
                ));
                fs::rename(&temp_path, final_path).unwrap();

                delete_tx.send(block_range.to).unwrap();
            };

            while let Some(elems) = rx.blocking_recv() {
                let row = row_group.last_mut().unwrap();

                let row = if row.len() >= cfg.items_per_row_group {
                    if row_group.iter().map(IntoRowGroups::len).sum::<usize>() >= cfg.items_per_file
                    {
                        write_group(&mut row_group);
                    }
                    row_group.push(T::default());
                    row_group.last_mut().unwrap()
                } else {
                    row
                };

                for elem in elems {
                    row.push(elem).unwrap();
                }
            }

            if row_group.last_mut().unwrap().len() > 0 {
                write_group(&mut row_group);
            }
        });

        Self {
            tx,
            _join_handle: join_handle,
        }
    }

    pub async fn send(&self, elems: Vec<T::Elem>) {
        if self.tx.send(elems).await.is_err() {
            panic!("failed to send elems to parquet writer");
        }
    }

    pub fn _join(self) {
        mem::drop(self.tx);
        self._join_handle.join().unwrap();
    }
}
