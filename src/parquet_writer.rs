use crate::types::WriteToParquet;
use crate::{Error, Result};
use parquet::basic::Repetition;
use parquet::basic::Type as BasicType;
use parquet::schema::types::{Type, TypePtr};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::mpsc;

pub struct ParquetWriter<T: WriteToParquet> {
    tx: mpsc::UnboundedSender<T>,
}

impl<T: WriteToParquet> ParquetWriter<T> {
    pub fn new<P: AsRef<Path>>(path: P, schema: TypePtr) -> Result<Self> {
        let (tx, rx) = mpsc::unbounded_channel();

        let task = WriteTask::new(path.as_ref(), rx, schema)?;

        tokio::task::spawn_blocking(|| async move {
            task.run().await;
        });

        Ok(Self { tx })
    }

    pub fn write(&self, val: T) {
        self.tx.send(val).unwrap();
    }
}

struct WriteTask<T: WriteToParquet> {
    path: PathBuf,
    file: File,
    rx: mpsc::UnboundedReceiver<T>,
    next_file_idx: usize,
    schema: TypePtr,
}

impl<T: WriteToParquet> WriteTask<T> {
    fn new(path: &Path, rx: mpsc::UnboundedReceiver<T>, schema: TypePtr) -> Result<Self> {
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

    async fn run(mut self) {
        while let Some(block) = self.rx.recv().await {
            
        }
    }
}
