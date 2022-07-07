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
            number bigint,
            hash blob,
            parent_hash blob,
            nonce blob,
            sha3_uncles blob,
            logs_bloom blob,
            transactions_root blob,
            state_root blob,
            receipts_root blob,
            miner blob,
            difficulty blob,
            total_difficulty blob,
            extradata blob,
            size bigint,
            gas_limit blob,
            gas_used blob,
            timestamp bigint,
            uncles blob,
            PRIMARY KEY (number)
        );
    ",
            &[],
        )
        .await
        .map_err(Error::CreateBlockTable)?;

    session
        .query(
            "
        CREATE TABLE IF NOT EXISTS eth.tx (
            hash blob,
            nonce blob,
            block_hash blob,
            block_number bigint,
            transaction_index blob,
            sender blob,
            receiver blob,
            value blob,
            gas_price blob,
            gas blob,
            input blob,
            v blob,
            standard_v boolean,
            r blob,
            raw blob,
            public_key blob,
            chain_id blob,
            PRIMARY KEY (block_number, transaction_index)
        );
    ",
            &[],
        )
        .await
        .map_err(Error::CreateTxTable)?;

    session
        .query(
            "
        CREATE TABLE IF NOT EXISTS eth.log (
            removed boolean,
            log_index blob,
            transaction_index blob,
            transaction_hash blob,
            block_hash blob,
            block_number bigint,
            address blob,
            data blob,
            topic0 blob,
            topic1 blob,
            topic2 blob,
            topic3 blob,
            PRIMARY KEY (block_number, log_index)
        );
    ",
            &[],
        )
        .await
        .map_err(Error::CreateLogTable)?;

    Ok(())
}
