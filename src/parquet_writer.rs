use crate::types::WriteToParquet;
use crate::{Error, Result};
use crossbeam::channel;
use parquet::basic::Repetition;
use parquet::basic::Type as BasicType;
use parquet::schema::types::{Type, TypePtr};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub struct ParquetWriter<T: WriteToParquet> {
    tx: channel::Sender<Vec<T>>,
}

impl<T: WriteToParquet> ParquetWriter<T> {
    pub fn new<P: AsRef<Path>>(path: P, schema: TypePtr) -> Result<Self> {
        let (tx, rx) = channel::unbounded();

        let task = WriteTask::new(path.as_ref(), rx, schema)?;

        std::thread::spawn(|| async move {
            task.run();
        });

        Ok(Self { tx })
    }

    pub fn write(&self, chunk: Vec<T>) {
        self.tx.send(chunk).unwrap();
    }
}

struct WriteTask<T: WriteToParquet> {
    path: PathBuf,
    file: File,
    rx: channel::Receiver<Vec<T>>,
    next_file_idx: usize,
    schema: TypePtr,
}

impl<T: WriteToParquet> WriteTask<T> {
    fn new(path: &Path, rx: channel::Receiver<Vec<T>>, schema: TypePtr) -> Result<Self> {
        let mut path = path.to_path_buf();
        path.push(format!("{}{}.parquet", schema.name(), 0));
        let file = File::create(path.as_path()).map_err(Error::CreateParquetFile)?;
        path.pop();

        Ok(Self {
            path,
            file,
            next_file_idx: 1,
            schema,
            rx,
        })
    }

    fn run(mut self) {
        while let Ok(chunk) = self.rx.recv() {}
    }
}
