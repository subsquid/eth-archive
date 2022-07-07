use crate::config::Config;
use crate::db::DbHandle;
use crate::{Error, Result};
use std::path::Path;
use tokio::fs;

pub struct Ingester {
    db_handle: DbHandle,
}

impl Ingester {
    pub async fn from_cfg_path(cfg_path: impl AsRef<Path>) -> Result<Self> {
        let config = fs::read_to_string(&cfg_path)
            .await
            .map_err(Error::ReadConfigFile)?;

        let config: Config = toml::de::from_str(&config).map_err(Error::ParseConfig)?;

        Self::from_cfg(&config).await
    }

    pub async fn from_cfg(config: &Config) -> Result<Self> {
        let db_handle = DbHandle::new(&config.db)
            .await
            .map_err(|e| Error::CreateDbHandle(Box::new(e)))?;

        Ok(Self { db_handle })
    }
}
