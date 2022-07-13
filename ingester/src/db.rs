use crate::config::DbConfig;
use crate::options::Options;
use crate::schema::Block;
use crate::{Error, Result};
use deadpool_postgres::Pool;
use std::sync::Arc;

mod embedded {
    use refinery::embed_migrations;
    embed_migrations!("migrations");
}

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

        let mut conn = pool.get().await.map_err(Error::GetDbConnection)?;

        if options.reset_db {
            reset_db(&conn).await?;
        }

        embedded::migrations::runner()
            .run_async(&mut **conn)
            .await
            .map_err(Error::RunMigrations)?;

        Ok(Self { pool })
    }

    async fn get_conn(&self) -> Result<deadpool_postgres::Object> {
        self.pool.get().await.map_err(Error::GetDbConnection)
    }

    pub async fn get_max_block_number(&self) -> Result<Option<usize>> {
        todo!();
    }

    pub async fn insert_blocks(&self, blocks: Arc<[Block]>) -> Result<()> {
        todo!();
    }
}

async fn reset_db(conn: &deadpool_postgres::Object) -> Result<()> {
    conn.execute(
        "
        DELETE FROM eth_log;
    ",
        &[],
    )
    .await
    .map_err(Error::ResetDb)?;

    conn.execute(
        "
        DELETE FROM eth_tx;
    ",
        &[],
    )
    .await
    .map_err(Error::ResetDb)?;

    conn.execute(
        "
        DELETE FROM eth_block;
    ",
        &[],
    )
    .await
    .map_err(Error::ResetDb)?;

    Ok(())
}
