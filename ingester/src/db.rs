use crate::config::DbConfig;
use crate::options::Options;
use crate::{Error, Result};

use scylla::statement::batch::Batch;
use scylla::statement::prepared_statement::PreparedStatement;
use scylla::{Session, SessionBuilder};

pub struct DbHandle {
    session: Session,
    queries: Queries,
}

struct Queries {
    insert_block: PreparedStatement,
}

impl Queries {
    async fn new(session: Session) -> Result<Self> {
        let insert_block = session
            .prepare(
                "
            INSERT INTO eth.block (
                number,
                hash,
                parent_hash,
                nonce,
                sha3_uncles,
                logs_bloom,
                transactions_root,
                state_root,
                receipts_root,
                miner,
                difficulty,
                total_difficulty,
                extradata,
                size,
                gas_limit,
                gas_used,
                timestamp,
                uncles
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        ",
            )
            .await
            .map_err(Error::PrepareInsertBlockStatement)?;

        Self { insert_block }
    }
}

impl DbHandle {
    pub async fn new(options: &Options, cfg: &DbConfig) -> Result<Self> {
        let mut session = SessionBuilder::new()
            .known_nodes(&cfg.known_nodes)
            .compression(cfg.connection_compression.map(Into::into));

        if let Some(auth) = &cfg.auth {
            session = session.user(&auth.username, &auth.password);
        }

        let session = session.build().await.map_err(Error::BuildDbSession)?;

        if options.reset_db {
            reset_db(&session)
                .await
                .map_err(|e| Error::ResetDb(Box::new(e)))?;
        }

        init_schema(&session)
            .await
            .map_err(|e| Error::InitSchema(Box::new(e)))?;

        let queries = Queries::new(&session).await?;

        Ok(Self { session, queries })
    }

    pub async fn get_max_block_number(&self) -> Result<Option<u64>> {
        let res = self
            .session
            .query("SELECT MAX(number) from block;", &[])
            .await
            .map_err(|e| Error::GetMaxBlockNumber(Box::new(e)))?;
        let (num,) = res
            .single_row_typed::<(Option<i64>,)>()
            .map_err(|e| Error::GetMaxBlockNumber(Box::new(e)))?;
        match num {
            Some(num) => {
                let num = u64::try_from(num).map_err(|e| Error::GetMaxBlockNumber(Box::new(e)))?;
                Ok(Some(num))
            }
            None => Ok(None),
        }
    }

    pub async fn insert_blocks(&self, blocks: Vec<Block>) -> Result<()> {
        let batch = Batch::new();

        for _ in 0..blocks.len() {
            batch.append(self.queries.insert_block.clone())
        }

        let batch_values = blocks
            .into_iter()
            .map(|block| {
                (
                    block.number,
                    block.hash,
                    block.parent_hash,
                    block.nonce,
                    block.sha3_uncles,
                    block.logs_bloom,
                    block.transactions_root,
                    block.state_root,
                    block.receipts_root,
                    block.miner,
                    block.difficulty,
                    block.total_difficulty,
                    block.extradata,
                    block.size,
                    block.gas_limit,
                    block.gas_used,
                    block.timestamp,
                    block.uncles,
                )
            })
            .collect();

        self.session.batch().await.map_err(Error::InsertBlocks)?;

        Ok(())
    }
}

async fn reset_db(session: &Session) -> Result<()> {
    session
        .query(
            "
        DROP KEYSPACE IF EXISTS eth;
    ",
            &[],
        )
        .await
        .map_err(Error::DropEthKeyspace)?;

    Ok(())
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
