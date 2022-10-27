use crate::{Error, Result};
use std::fmt;
use std::str::FromStr;
use rayon::prelude::*;
use std::path::Path;
use crate::types::BlockRange;
use crate::rayon_async;

const FOLDER_PREFIX: &str = "blk";
const TEMP_SUFFIX: &str = "temp";

#[derive(Debug, Clone, Copy)]
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
            None => (input, true),
            Some(input) => {
                (input, true)
            },
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
        write!(f, "{}{}-{}", FOLDER_PREFIX, self.range.from, self.range.to)?;
    
        if self.is_temp {
            f.write_str(TEMP_SUFFIX)?;
        }

        Ok(())
    }
}

impl DirName {
    pub async fn list_sorted_folder_names<P: AsRef<Path>>(path: P) -> Result<Vec<DirName>> {
        log::info!("listing folder names...");

        let mut dir = tokio::fs::read_dir(&path)
            .await
            .map_err(Error::ReadParquetDir)?;

        let mut names = Vec::new();
        while let Some(entry) = dir.next_entry().await.map_err(Error::ReadParquetDir)? {
            let folder_name = entry.file_name();
            let folder_name = folder_name.to_str().ok_or(Error::InvalidFolderName)?;
            names.push(DirName::from_str(folder_name)?);
        }

        log::info!("soring folder names...");

        let sorted_names = rayon_async::spawn(move || {
            let mut names = names;
            names.par_sort_by_key(|name| name.range.from);
            names
        }).await;

        Ok(sorted_names)
    }
}
