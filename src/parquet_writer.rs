use tokio::sync::mpsc;
use crate::types::BlockData;

pub struct ParquetWriter {
	tx: mpsc::UnboundedSender<BlockData>,
}

impl ParquetWriter {
	pub fn new() -> Self {
		let (tx, rx) = mpsc::unbounded_channel();

		

		Self {
			tx,
		}
	}

	pub fn write(block: BlockData) {
		self.tx.send(block).unwrap();
	}
}
