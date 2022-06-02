use crate::schema::IntoRowGroups;
use arrow2::io::parquet::write::*;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::{fs, mem};

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
        let (tx, rx) = mpsc::channel();

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

                let mut path = path.clone();
                path.push(format!("{}{}.parquet", &name, file_idx));
                let file = fs::File::create(&path).unwrap();
                let mut writer = FileWriter::try_new(file, schema, options).unwrap();

                writer.start().unwrap();
                for group in row_groups {
                    writer.write(group.unwrap()).unwrap();
                }
                writer.end(None).unwrap();

                file_idx += 1;
            };

            while let Ok(elems) = rx.recv() {
                let row = row_group.last_mut().unwrap();

                let row = if row.len() == row_group_size {
                    if row_group.len() * row_group_size == threshold {
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

    pub fn send(&self, elems: Vec<T::Elem>) {
        self.tx.send(elems).unwrap();
    }

    pub fn join(self) {
        mem::drop(self.tx);
        self.join_handle.join().unwrap();
    }
}
