use crate::schema::IntoRowGroups;
use arrow2::io::parquet::write::*;
use std::sync::mpsc;
use std::{fs, mem};

pub struct ParquetWriter<T: IntoRowGroups> {
    tx: mpsc::Sender<T::Elem>,
}

impl<T: IntoRowGroups> ParquetWriter<T> {
    pub fn new(path: String, threshold: usize) -> Self {
        let (tx, rx) = mpsc::channel();

        std::thread::spawn(move || {
            let mut elems = T::default();
            let mut file_idx = 0;

            while let Ok(elem) = rx.recv() {
                elems.push(elem);

                if elems.len() > threshold {
                    let elems = mem::take(&mut elems);
                    let (row_groups, schema, options) = elems.into_row_groups();

                    let file = fs::File::create(&format!("{}{}", path, file_idx,)).unwrap();
                    let mut writer = FileWriter::try_new(file, schema, options).unwrap();

                    writer.start().unwrap();
                    for group in row_groups {
                        writer.write(group.unwrap()).unwrap();
                    }
                    writer.end(None).unwrap();

                    file_idx += 1;
                }
            }
        });

        Self { tx }
    }

    pub fn send(&self, elem: T::Elem) {
        self.tx.send(elem).unwrap();
    }
}
