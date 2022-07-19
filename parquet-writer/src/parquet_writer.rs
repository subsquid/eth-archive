use crate::schema::IntoRowGroups;
use arrow2::io::parquet::write::*;
use std::path::{Path, PathBuf};
use std::{fs, mem};
use tokio::sync::mpsc;

pub struct ParquetWriter<T: IntoRowGroups> {
    tx: mpsc::Sender<Vec<T::Elem>>,
    join_handle: std::thread::JoinHandle<()>,
}

impl<T: IntoRowGroups> ParquetWriter<T> {
    pub fn new<P: AsRef<Path>, S: Into<String>>(
        name: S,
        path: P,
        row_group_size: usize,
        threshold: usize,
    ) -> Self {
        let (tx, mut rx) = mpsc::channel(512);

        let name = name.into();

        let mut path: PathBuf = path.as_ref().into();
        path.push(&name);
        let path = path;

        fs::create_dir_all(&path).unwrap();

        let join_handle = std::thread::spawn(move || {
            let mut row_group = vec![T::default()];
            let mut file_idx = 0;

            let mut write_group = |row_group: &mut Vec<T>| {
                let row_group = mem::take(row_group);
                let (row_groups, schema, options) = T::into_row_groups(row_group);

                let mut temp_path = path.clone();
                temp_path.push(format!("{}{}.temp", &name, file_idx));
                let file = fs::File::create(&temp_path).unwrap();
                let mut writer = FileWriter::try_new(file, schema, options).unwrap();

                writer.start().unwrap();
                for group in row_groups {
                    writer.write(group.unwrap()).unwrap();
                }
                writer.end(None).unwrap();

                let mut final_path = path.clone();
                final_path.push(format!("{}{}.parquet", &name, file_idx));
                fs::rename(&temp_path, final_path).unwrap();

                file_idx += 1;
            };

            while let Some(elems) = rx.blocking_recv() {
                let row = row_group.last_mut().unwrap();

                let row = if row.len() >= row_group_size {
                    if row_group.iter().map(IntoRowGroups::len).sum::<usize>() >= threshold {
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

        Self { tx, join_handle }
    }

    pub async fn send(&self, elems: Vec<T::Elem>) {
        self.tx.send(elems).await.unwrap();
    }

    pub fn join(self) {
        mem::drop(self.tx);
        self.join_handle.join().unwrap();
    }
}