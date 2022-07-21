use crate::options::Options;
use crate::{Error, Result};
use deadpool_postgres::Pool;
use eth_archive_core::config::DbConfig;
use eth_archive_core::types::Block;
use std::sync::Arc;

pub struct DbHandle {
    pool: Pool,
}

impl DbHandle {
    pub async fn new(options: &Options, cfg: &DbConfig) -> Result<Self> {
        use deadpool_postgres::{Config, Runtime};

        let cfg = Config {
            user: Some(cfg.user.clone()),
            password: Some(cfg.password.clone()),
            dbname: Some(cfg.dbname.clone()),
            host: Some(cfg.host.clone()),
            port: Some(cfg.port),
            ..Config::default()
        };

        let pool = cfg
            .create_pool(Some(Runtime::Tokio1), tokio_postgres::NoTls)
            .map_err(Error::CreatePool)?;

        Ok(Self { pool })
    }

    async fn get_conn(&self) -> Result<deadpool_postgres::Object> {
        self.pool.get().await.map_err(Error::GetDbConnection)
    }

    pub async fn get_min_block_number(&self) -> Result<Option<usize>> {
        let rows = self
            .get_conn()
            .await?
            .query("SELECT MIN(number) from eth_block;", &[])
            .await
            .map_err(Error::DbQuery)?;
        let row = match rows.get(0) {
            Some(row) => row,
            None => return Ok(None),
        };
        match row.get::<_, Option<i64>>(0) {
            Some(num) => Ok(Some(num as usize)),
            None => Ok(None),
        }
    }
}
