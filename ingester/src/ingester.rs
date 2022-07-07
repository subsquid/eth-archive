use crate::config::Config;
use crate::db::DbHandle;
use crate::options::Options;
use crate::{Error, Result};

pub struct Ingester {
    db_handle: DbHandle,
}

impl Ingester {
    pub async fn new(options: &Options) -> Result<Self> {
        let config = tokio::fs::read_to_string(&options.cfg_path)
            .await
            .map_err(Error::ReadConfigFile)?;

        let config: Config = toml::de::from_str(&config).map_err(Error::ParseConfig)?;

        let db_handle = DbHandle::new(options, &config.db)
            .await
            .map_err(|e| Error::CreateDbHandle(Box::new(e)))?;

        Ok(Self { db_handle })
    }
}
