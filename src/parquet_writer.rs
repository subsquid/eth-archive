use crate::schema::IntoRowGroups;
use arrow2::io::parquet::write::*;
use std::sync::mpsc;
use std::{fs, mem};

pub struct ParquetWriter<T: IntoRowGroups> {
    tx: mpsc::Sender<Vec<T::Elem>>,
}

impl<T: IntoRowGroups> ParquetWriter<T> {
    pub fn new<S: Into<String>>(path: S, threshold: usize) -> Self {
        let (tx, rx) = mpsc::channel();

        let path = path.into();

        std::thread::spawn(move || {
            let mut row_group = T::default();
            let mut file_idx = 0;

            let mut write_group = |row_group: &mut T| {
                let row_group = mem::take(row_group);
                let (row_groups, schema, options) = row_group.into_row_groups();

                let file = fs::File::create(&format!("{}{}.parquet", path, file_idx,)).unwrap();
                let mut writer = FileWriter::try_new(file, schema, options).unwrap();

                writer.start().unwrap();
                for group in row_groups {
                    writer.write(group.unwrap()).unwrap();
                }
                writer.end(None).unwrap();

                file_idx += 1;
            };

            while let Ok(elems) = rx.recv() {
                for elem in elems {
                    row_group.push(elem).unwrap();
                }

                if row_group.len() > threshold {
                    write_group(&mut row_group);
                }
            }

            write_group(&mut row_group);
        });

        Self { tx }
    }

    pub fn send(&self, elems: Vec<T::Elem>) {
        self.tx.send(elems).unwrap();
    }
}
