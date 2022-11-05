use crate::rayon_async;
use crate::types::BlockRange;
use crate::{Error, Result};
use rayon::prelude::*;
use std::fmt;
use std::path::Path;
use std::str::FromStr;

const FOLDER_PREFIX: &str = "blk";
const TEMP_SUFFIX: &str = "temp";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DirName {
    pub range: BlockRange,
    pub is_temp: bool,
}

impl FromStr for DirName {
    type Err = Error;

    fn from_str(input: &str) -> Result<DirName> {
        let err = || Error::InvalidBlockRange(input.to_owned());

        let input = input.strip_prefix(FOLDER_PREFIX).ok_or_else(err)?;

        let (input, is_temp) = match input.strip_suffix(TEMP_SUFFIX) {
            None => (input, false),
            Some(input) => (input, true),
        };

        let mut input = input.split('-');
        let from = input.next().ok_or_else(err)?;
        let to = input.next().ok_or_else(err)?;
        if input.count() > 0 {
            return Err(err());
        }

        let from = u32::from_str(from).map_err(|_| err())?;
        let to = u32::from_str(to).map_err(|_| err())?;

        Ok(Self {
            range: BlockRange { from, to },
            is_temp,
        })
    }
}

impl fmt::Display for DirName {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}{:09}-{:09}",
            FOLDER_PREFIX, self.range.from, self.range.to
        )?;

        if self.is_temp {
            f.write_str(TEMP_SUFFIX)?;
        }

        Ok(())
    }
}

impl DirName {
    pub async fn delete_temp_and_list_sorted<P: AsRef<Path>>(path: P) -> Result<Vec<DirName>> {
        log::info!("listing folder names...");

        let mut dir = tokio::fs::read_dir(&path)
            .await
            .map_err(Error::ReadParquetDir)?;

        let mut names = Vec::new();
        while let Some(entry) = dir.next_entry().await.map_err(Error::ReadParquetDir)? {
            let folder_name = entry.file_name();
            let folder_name = folder_name.to_str().ok_or(Error::InvalidFolderName)?;
            let dir_name = DirName::from_str(folder_name)?;
            if dir_name.is_temp {
                log::info!("deleting temp dir {}...", dir_name);

                tokio::fs::remove_dir_all(&entry.path())
                    .await
                    .map_err(Error::RemoveTempDir)?;
            } else {
                names.push(dir_name);
            }
        }

        log::info!("sorting folder names...");

        let sorted_names = rayon_async::spawn(move || {
            let mut names = names;
            names.par_sort_by_key(|name| name.range.from);
            names
        })
        .await;

        Ok(sorted_names)
    }

    pub async fn find_sorted<P: AsRef<Path>>(path: P, from: u32) -> Result<Vec<DirName>> {
        let mut dir = tokio::fs::read_dir(&path)
            .await
            .map_err(Error::ReadParquetDir)?;

        let mut names = Vec::new();
        while let Some(entry) = dir.next_entry().await.map_err(Error::ReadParquetDir)? {
            let folder_name = entry.file_name();
            let folder_name = folder_name.to_str().ok_or(Error::InvalidFolderName)?;
            let dir_name = DirName::from_str(folder_name)?;

            if !dir_name.is_temp {
                names.push(dir_name);
            }
        }

        let sorted_names = rayon_async::spawn(move || {
            let mut names = names;
            names.par_sort_by_key(|name| name.range.from);
            names
        })
        .await;

        let mut next = from;
        let mut dir_names = Vec::new();

        for name in sorted_names
            .into_iter()
            .skip_while(|name| name.range.from != from)
        {
            if name.range.from == next {
                next = name.range.to;
                dir_names.push(name);
            } else {
                break;
            }
        }

        Ok(dir_names)
    }
}
