use crate::config::DbConfig;
use crate::options::Options;
use crate::schema::Block;
use crate::{Error, Result};
use deadpool_postgres::Pool;
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

        let conn = pool.get().await.map_err(Error::GetDbConnection)?;

        if options.reset_db {
            if let Err(e) = reset_db(&conn).await {
                log::error!("{}", e);
            }
        }

        init_db(&conn).await?;

        Ok(Self { pool })
    }

    async fn get_conn(&self) -> Result<deadpool_postgres::Object> {
        self.pool.get().await.map_err(Error::GetDbConnection)
    }

    pub async fn get_max_block_number(&self) -> Result<Option<usize>> {
        let rows = self
            .get_conn()
            .await?
            .query("SELECT MAX(number) from eth_block;", &[])
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

    pub async fn insert_blocks(&self, blocks: Arc<[Block]>) -> Result<()> {
        let mut conn = self.get_conn().await?;

        let tx = conn.transaction().await.map_err(Error::CreateDbTransaction)?;

        for block in blocks.iter() {
            tx.execute("INSERT INTO eth_block (
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
                extra_data,
                size,
                gas_limit,
                gas_used,
                timestamp,
                uncles
            ) VALUES (
                $1,
                $2,
                $3,
                $4,
                $5,
                $6,
                $7,
                $8,
                $9,
                $10,
                $11,
                $12,
                $13,
                $14,
                $15,
                $16,
                $17,
                $18
            );", &[
                &block.number,
                &block.hash,
                &block.parent_hash,
                &block.nonce,
                &block.sha3_uncles,
                &block.logs_bloom,
                &block.transactions_root,
                &block.state_root,
                &block.receipts_root,
                &block.miner,
                &block.difficulty,
                &block.total_difficulty,
                &block.extra_data,
                &block.size,
                &block.gas_limit,
                &block.gas_used,
                &block.timestamp,
                &block.uncles
            ]).await.map_err(Error::InsertBlock)?;
        }

        Ok(())
    }
}

async fn reset_db(conn: &deadpool_postgres::Object) -> Result<()> {
    conn.batch_execute(
        "
        DELETE FROM eth_log;
        DELETE FROM eth_tx;
        DELETE FROM eth_block;
    ",
    )
    .await
    .map_err(Error::ResetDb)?;

    Ok(())
}

async fn init_db(conn: &deadpool_postgres::Object) -> Result<()> {
    conn.batch_execute(
        "
        CREATE TABLE IF NOT EXISTS eth_block (
            row_id BIGSERIAL PRIMARY KEY,
            number bigint,
            hash bytea,
            parent_hash bytea,
            nonce bytea,
            sha3_uncles bytea,
            logs_bloom bytea,
            transactions_root bytea,
            state_root bytea,
            receipts_root bytea,
            miner bytea,
            difficulty bytea,
            total_difficulty bytea,
            extra_data bytea,
            size bigint,
            gas_limit bytea,
            gas_used bytea,
            timestamp bigint,
            uncles bytea
        );
        
        CREATE TABLE IF NOT EXISTS eth_tx (
            row_id BIGSERIAL PRIMARY KEY,
            hash bytea,
            nonce bytea,
            block_hash bytea,
            block_number bigint,
            transaction_index bytea,
            sender bytea,
            receiver bytea,
            value bytea,
            gas_price bytea,
            gas bytea,
            input bytea,
            v bytea,
            standard_v boolean,
            r bytea,
            raw bytea,
            public_key bytea,
            chain_id bytea,
            block_row_id BIGINT NOT NULL REFERENCES eth_block(row_id)
        );
        
        CREATE TABLE IF NOT EXISTS eth_log (
            row_id BIGSERIAL PRIMARY KEY,
            removed boolean,
            log_index bytea,
            transaction_index bytea,
            transaction_hash bytea,
            block_hash bytea,
            block_number bigint,
            address bytea,
            data bytea,
            topic0 bytea,
            topic1 bytea,
            topic2 bytea,
            topic3 bytea,
            block_row_id BIGINT NOT NULL REFERENCES eth_block(row_id)
        );
    ",
    )
    .await
    .map_err(Error::InitDb)?;

    Ok(())
}
