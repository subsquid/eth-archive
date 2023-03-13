use crate::db::DbHandle;
use crate::db_writer::DbWriter;
use crate::{Error, Result};
use eth_archive_core::dir_name::DirName;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

pub struct ParquetWatcher {
    pub db: Arc<DbHandle>,
    pub data_path: PathBuf,
    pub db_writer: Arc<DbWriter>,
}

impl ParquetWatcher {
    pub async fn spawn(self) -> Result<()> {
        let start = self.db.parquet_height();

        let data_path = self.data_path;

        tokio::fs::create_dir_all(&data_path)
            .await
            .map_err(Error::CreateMissingDirectories)?;

        let db_writer = self.db_writer;

        tokio::spawn(async move {
            let mut next_start = start;
            loop {
                let dir_names = DirName::find_sorted(&data_path, next_start).await.unwrap();

                let mut to_register = Vec::new();

                for dir_name in dir_names {
                    if !Self::parquet_folder_is_valid(&data_path, dir_name)
                        .await
                        .unwrap()
                    {
                        break;
                    }

                    to_register.push(dir_name);
                    next_start = dir_name.range.to;
                }

                if !to_register.is_empty() {
                    db_writer.register_parquet_folders(to_register).await;
                }

                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        });

        Ok(())
    }

    async fn parquet_folder_is_valid(data_path: &Path, dir_name: DirName) -> Result<bool> {
        let mut path = data_path.to_owned();
        path.push(dir_name.to_string());

        for name in ["block", "tx", "log"] {
            let mut path = path.clone();
            path.push(format!("{name}.parquet"));
            match tokio::fs::File::open(&path).await {
                Err(e) if e.kind() == io::ErrorKind::NotFound => {
                    return Ok(false);
                }
                Err(e) => return Err(Error::ReadParquetDir(e)),
                Ok(_) => (),
            }
        }

        Ok(true)
    }
}
