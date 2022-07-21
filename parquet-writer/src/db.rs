use crate::{Error, Result};
use deadpool_postgres::Pool;
use eth_archive_core::config::DbConfig;
use eth_archive_core::deserialize::{Address, BigInt, BloomFilterBytes, Bytes, Bytes32, Nonce};
use eth_archive_core::types::Block;

pub struct DbHandle {
    pool: Pool,
}

impl DbHandle {
    pub async fn new(cfg: &DbConfig) -> Result<Self> {
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

    pub async fn get_block(&self, block_number: i64) -> Result<Option<Block>> {
        let rows = self
            .get_conn()
            .await?
            .query(
                "SELECT (
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
                timestamp
            ) from eth_block
            WHERE number = $1;",
                &[&block_number],
            )
            .await
            .map_err(Error::DbQuery)?;
        let row = match rows.get(0) {
            Some(row) => row,
            None => return Ok(None),
        };

        Ok(Some(Block {
            number: BigInt(row.get(0)),
            hash: Bytes32::new(row.get(1)),
            parent_hash: Bytes32::new(row.get(2)),
            nonce: Nonce::new(row.get(3)),
            sha3_uncles: Bytes32::new(row.get(4)),
            logs_bloom: BloomFilterBytes::new(row.get(5)),
            transactions_root: Bytes32::new(row.get(6)),
            state_root: Bytes32::new(row.get(7)),
            receipts_root: Bytes32::new(row.get(8)),
            miner: Address::new(row.get(9)),
            difficulty: Bytes(row.get(10)),
            total_difficulty: Bytes(row.get(11)),
            extra_data: Bytes(row.get(12)),
            size: BigInt(row.get(13)),
            gas_limit: Bytes(row.get(14)),
            gas_used: Bytes(row.get(15)),
            timestamp: BigInt(row.get(16)),
            transactions: Vec::new(),
        }))
    }

    pub async fn delete_block(&self, block_number: i64) -> Result<()> {
        self.get_conn()
            .await?
            .execute("DELETE FROM eth_block WHERE number=$1", &[&block_number])
            .await
            .map_err(Error::DbQuery)?;
        Ok(())
    }
}
