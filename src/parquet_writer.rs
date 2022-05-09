use crate::types::BlockData;
use parquet::basic::Repetition;
use parquet::basic::Type as BasicType;
use parquet::schema::types::{Type, TypePtr};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::mpsc;

pub struct ParquetWriter {
    tx: mpsc::UnboundedSender<BlockData>,
}

impl ParquetWriter {
    pub fn new<P: AsRef<Path>, S: Into<String>>(name: S, path: P) -> Result<Self> {
        let (tx, rx) = mpsc::unbounded_channel();

        let task = WriteTask::new(name.into(), Path::from(path.as_ref()))?;

        tokio::spawn_blocking(|| async move {
            task.run();
        });

        Self { tx }
    }

    pub fn write(block: BlockData) {
        self.tx.send(block).unwrap();
    }
}

struct WriteTask {
    path: Path,
    file: File,
    rx: mpsc::UnboundedReceiver<BlockData>,
    next_file_idx: usize,
    name: String,
    schema: TypePtr,
}

impl WriteTask {
    fn new(name: String, path: Path) -> Result<Self> {
        let mut buf = self.path.to_path_buf();
        buf.push(format!("{}{}.parquet", &name, 0));

        let file = File::create(buf.as_path()).await?;

        let type_ptr = Type::group_type_builder(&name).with_fields(&mut vec![
            Arc::new(
                Type::primitive_type_builder("hash", BasicType::FIXED_LEN_BYTE_ARRAY),
                    .with_repetition(Repetition::REQUIRED)
                    .with_length(32)
                    .build()
                    .unwrap()
            ),
            Arc::new(
                Type::primitive_type_builder("nonce", BasicType::FIXED_LEN_BYTE_ARRAY),
                    .with_repetition(Repetition::REQUIRED)
                    .with_length(32)
                    .build()
                    .unwrap()
            ),
            Arc::new(
                Type::primitive_type_builder("block_hash", BasicType::FIXED_LEN_BYTE_ARRAY),
                    .with_repetition(Repetition::OPTIONAL)
                    .with_length(32)
                    .build()
                    .unwrap()
            ),
            Arc::new(
                Type::primitive_type_builder("block_number", BasicType::INT64),
                    .with_repetition(Repetition::OPTIONAL)
                    .build()
                    .unwrap()
            ),
            Arc::new(
                Type::primitive_type_builder("transaction_index", BasicType::INT64),
                    .with_repetition(Repetition::OPTIONAL)
                    .build()
                    .unwrap()
            ),
            Arc::new(
                Type::primitive_type_builder("from", BasicType::FIXED_LEN_BYTE_ARRAY),
                    .with_repetition(Repetition::OPTIONAL)
                    .with_length(20)
                    .build()
                    .unwrap()
            ),
            Arc::new(
                Type::primitive_type_builder("to", BasicType::FIXED_LEN_BYTE_ARRAY),
                    .with_repetition(Repetition::OPTIONAL)
                    .with_length(20)
                    .build()
                    .unwrap()
            ),
            Arc::new(
                Type::primitive_type_builder("value", BasicType::FIXED_LEN_BYTE_ARRAY),
                    .with_repetition(Repetition::REQUIRED)
                    .with_length(32)
                    .build()
                    .unwrap()
            ),
            Arc::new(
                Type::primitive_type_builder("gas_price", BasicType::FIXED_LEN_BYTE_ARRAY),
                    .with_repetition(Repetition::OPTIONAL)
                    .with_length(32)
                    .build()asd
                    .unwrap()
            ),
            Arc::new(
                Type::primitive_type_builder("gas", BasicType::FIXED_LEN_BYTE_ARRAY),
                    .with_repetition(Repetition::REQUIRED)
                    .with_length(32)
                    .build()
                    .unwrap()
            ),
            Arc::new(
                Type::primitive_type_builder("input", BasicType::BYTE_ARRAY)
                    .with_repetition(Repetition::REQUIRED)
                    .build()
                    .unwrap()
            ),
            Arc::new(
                Type::primitive_type_builder("v", BasicType::INT64),
                    .with_repetition(Repetition::OPTIONAL)
                    .build()
                    .unwrap()
            ),
            Arc::new(
                Type::primitive_type_builder("r", BasicType::FIXED_LEN_BYTE_ARRAY),
                    .with_repetition(Repetition::OPTIONAL)
                    .with_length(32)
                    .build()
                    .unwrap()
            ),
               Arc::new(
                Type::primitive_type_builder("s", BasicType::FIXED_LEN_BYTE_ARRAY),
                    .with_repetition(Repetition::OPTIONAL)
                    .with_length(32)
                    .build()
                    .unwrap()
            ),
            Arc::new(
                Type::primitive_type_builder("raw", BasicType::BYTE_ARRAY)
                    .with_repetition(Repetition::OPTIONAL)
                    .build()
                    .unwrap()
            ),
            Arc::new(
                Type::primitive_type_builder("transaction_type", BasicType::INT64),
                    .with_repetition(Repetition::OPTIONAL)
                    .build()
                    .unwrap()
            ),
        ]);

        let type_ptr = Arc::new(type_ptr);

        Self {
            path,
            file,
            file_idx: 1,
            name,
            type_ptr,
        }
    }

    async fn run(self) {
        while let Some(block) = self.rx.recv() {
            self.file.write
        }
    }

    fn open_new_file(&self) -> Result<File> {
        let mut buf = self.path.to_path_buf();
        buf.push()
    }
}
