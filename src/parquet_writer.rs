use tokio::sync::mpsc;
use crate::types::BlockData;
use std::path::{Path, PathBuf};
use std::fs::File;

pub struct ParquetWriter {
	tx: mpsc::UnboundedSender<BlockData>,
}

impl ParquetWriter {
	pub fn new<P: AsRef<Path>, S: Into<String>>(name: S, path: P) -> Result<Self> {
		let (tx, rx) = mpsc::unbounded_channel();

		let task = WriteTask::new(name.into(), Path::from(path.as_ref()))?;

		tokio::spawn_blocking(|| async move { task.run(); });

		Self {
			tx,
		}
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
}

impl WriteTask {
	fn new(name: String, path: Path) -> Result<Self> {
		let mut buf = self.path.to_path_buf();
		buf.push(format!("{}{}.patquet", &name, 0));

		let file = File::create(buf.as_path()).await?;

		Self {
			path, file, file_idx: 1, name,
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
