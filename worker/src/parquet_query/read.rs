use crate::{Error, Result};
use arrow2::array::Array;
use arrow2::chunk::Chunk;
use arrow2::datatypes::Field;
use arrow2::io::parquet;
use arrow2::io::parquet::read::ArrayIter;
use eth_archive_core::hash::HashMap;
use eth_archive_core::rayon_async;
use futures::future::BoxFuture;
use rayon::prelude::*;
use std::path::PathBuf;
use tokio::fs::File;
use tokio::io::BufReader;
use tokio_util::compat::TokioAsyncReadCompatExt;

pub struct ReadParquet<F: Fn(usize) -> bool> {
    pub path: PathBuf,
    pub rg_filter: F,
    pub fields: Vec<Field>,
}

fn deserialize_parallel(
    iters: &mut [ArrayIter<'static>],
) -> arrow2::error::Result<Chunk<Box<dyn Array>>> {
    let arrays = iters
        .par_iter_mut()
        .map(|iter| iter.next().transpose())
        .collect::<arrow2::error::Result<Vec<_>>>()?;

    Chunk::try_new(arrays.into_iter().map(|x| x.unwrap()).collect())
}

const CHUNK_SIZE: usize = 1024 * 8 * 8;

impl<F: Fn(usize) -> bool> ReadParquet<F> {
    pub async fn read(self) -> Result<ChunkReceiver> {
        let metadata = {
            let mut reader = BufReader::new(
                File::open(&self.path)
                    .await
                    .map_err(Error::OpenParquetFile)?,
            )
            .compat();
            parquet::read::read_metadata_async(&mut reader)
                .await
                .map_err(Error::ReadParquet)?
        };

        let (tx, rx) = crossbeam::channel::unbounded();

        for (i, rg_meta) in metadata.row_groups.into_iter().enumerate() {
            if (self.rg_filter)(i) {
                let fields = self.fields.clone();
                let tx = tx.clone();
                let path = self.path.clone();
                tokio::task::spawn(async move {
                    let open_reader = move || {
                        let path = path.clone();
                        Box::pin(async move {
                            let file = File::open(&path).await?.compat();
                            Ok(file)
                        }) as BoxFuture<_>
                    };

                    let mut columns = match parquet::read::read_columns_many_async(
                        open_reader,
                        &rg_meta,
                        fields.clone(),
                        Some(CHUNK_SIZE),
                        None,
                        None,
                    )
                    .await
                    .map_err(Error::ReadParquet)
                    {
                        Ok(columns) => columns,
                        Err(e) => {
                            tx.send(Err(e)).ok();
                            return;
                        }
                    };

                    let mut num_rows = rg_meta.num_rows();
                    rayon_async::spawn(move || {
                        while num_rows > 0 {
                            num_rows = num_rows.saturating_sub(CHUNK_SIZE);
                            let chunk =
                                deserialize_parallel(&mut columns).map_err(Error::ReadParquet);

                            let chunk = chunk.map(|c| {
                                let c = c
                                    .into_arrays()
                                    .into_iter()
                                    .zip(fields.iter())
                                    .map(|(col, field)| (field.name.to_owned(), col))
                                    .collect::<HashMap<_, _>>();

                                (i, c)
                            });

                            tx.send(chunk).ok();
                        }
                    })
                    .await;
                });
            }
        }

        Ok(rx)
    }
}

type ChunkReceiver = crossbeam::channel::Receiver<ChunkRes>;
type ChunkRes = Result<(usize, HashMap<String, Box<dyn Array>>)>;
