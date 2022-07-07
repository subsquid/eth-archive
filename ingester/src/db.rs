use crate::config::DbConfig;
use crate::{Error, Result};

use scylla::{Session, SessionBuilder};

pub struct DbHandle {
    session: Session,
}

impl DbHandle {
    pub async fn new(cfg: &DbConfig) -> Result<Self> {
        let mut session = SessionBuilder::new()
            .known_nodes(&cfg.known_nodes)
            .compression(cfg.compression.map(Into::into));

        if let Some(auth) = &cfg.auth {
            session = session.user(&auth.username, &auth.password);
        }

        let session = session.build().await.map_err(Error::BuildDbSession)?;

        init_schema(&session)
            .await
            .map_err(|e| Error::InitSchema(Box::new(e)))?;

        Ok(Self { session })
    }
}

async fn init_schema(session: &Session) -> Result<()> {
    session.query("
        CREATE KEYSPACE IF NOT EXISTS eth WITH REPLICATION = {
            'class': 'SimpleStrategy',
            'replication_factor': 1
        };
    ", &[]).await.map_err(Error::CreateKeyspace)?;

    session.query("
        CREATE TABLE IF NOT EXISTS block (
            number BIGINT NOT NULL,
            hash BLOB NOT NULL,
            
        );
    ", &[]).await.map_err(Error::CreateTable)?;
}
