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
    session
        .query(
            "
        CREATE KEYSPACE IF NOT EXISTS eth WITH REPLICATION = {
            'class': 'SimpleStrategy',
            'replication_factor': 1
        };
    ",
            &[],
        )
        .await
        .map_err(Error::CreateKeyspace)?;

    session
        .query(
            "
        CREATE TABLE IF NOT EXISTS eth.block (
            number BIGINT NOT NULL,
            hash BLOB NOT NULL,
            parent_hash BLOB NOT NULL,
            nonce BLOB NOT NULL,
            sha3_uncles BLOB NOT NULL,
            logs_bloom BLOB NOT NULL,
            transactions_root BLOB NOT NULL,
            state_root BLOB NOT NULL,
            receipts_root BLOB NOT NULL,
            miner BLOB NOT NULL,
            difficulty BLOB NOT NULL,
            total_difficulty BLOB NOT NULL,
            extradata BLOB NOT NULL,
            size BIGINT NOT NULL,
            gas_limit BLOB NOT NULL,
            gas_used BLOB NOT NULL,
            timestamp BIGINT NOT NULL,
            uncles BLOB NOT NULL
        );
    ",
            &[],
        )
        .await
        .map_err(Error::CreateTable)?;

    session
        .query(
            "
        CREATE INDEX IF NOT EXISTS ON eth.block(number);
    ",
            &[],
        )
        .await
        .map_err(Error::CreateTable)?;

    session
        .query(
            "
        CREATE TABLE IF NOT EXISTS eth.tx (
            hash BLOB NOT NULL,
            nonce BLOB NOT NULL,
            block_hash BLOB NOT NULL,
            block_number BIGINT NOT NULL,
            transaction_index BLOB NOT NULL,
            from BLOB NOT NULL,
            to BLOB,
            value BLOB NOT NULL,
            gas_price BLOB NOT NULL,
            gas BLOB NOT NULL,
            input BLOB NOT NULL,
            v BLOB NOT NULL,
            standard_v BOOLEAN NOT NULL,
            r BLOB NOT NULL,
            raw BLOB NOT NULL,
            public_key BLOB NOT NULL,
            chain_id BLOB NOT NULL
        );
    ",
            &[],
        )
        .await
        .map_err(Error::CreateTable)?;

    session
        .query(
            "
        CREATE INDEX IF NOT EXISTS ON eth.tx(block_number);
    ",
            &[],
        )
        .await
        .map_err(Error::CreateTable)?;

    session
        .query(
            "
        CREATE TABLE IF NOT EXISTS eth.log (
            removed BOOLEAN NOT NULL,
            log_index BLOB NOT NULL,
            transaction_index BLOB NOT NULL,
            transaction_hash BLOB NOT NULL,
            block_hash BLOB NOT NULL,
            block_number BIGINT NOT NULL,
            address BLOB NOT NULL,
            data BLOB NOT NULL,
            topic0 BLOB,
            topic1 BLOB,
            topic2 BLOB,
            topic3 BLOB
        );
    ",
            &[],
        )
        .await
        .map_err(Error::CreateTable)?;

    session
        .query(
            "
        CREATE INDEX IF NOT EXISTS ON eth.log(block_number);
    ",
            &[],
        )
        .await
        .map_err(Error::CreateTable)?;

    Ok(())
}
