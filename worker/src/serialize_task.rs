pub struct SerializeTask {
	tx: mpsc::Sender<(QueryResult, BlockRange)>,
	join_handle: tokio::task::JoinHandle<Result<Vec<u8>>>,
}

impl SerializeTask {
	pub fn new(from_block: u32, size_limit: usize) -> Self {
		let (tx, mut rx) = mpsc::channel(1);

		let join_handle = tokio::spawn(async move {
			let mut bytes = br#"{"data":["#.to_vec();

			let mut comma = false;

			let mut next_block = query.from_block;

			while let Some((res, range)) = rx.recv().await {
				next_block = range.to;

				bytes = rayon_async::spawn(move || {
					
				}).await;

				comma = true;
			}
		});

		Self {
			tx, join_handle,
		}
	}

	pub async fn join(self) -> Result<Vec<u8>> {
		let bytes = self.join_handle.await.map_err(Error::TaskJoinError)??;

		Ok(bytes)
	}
}
