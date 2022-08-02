use crate::config::DbConfig;
use crate::deserialize::{Address, BigInt, BloomFilterBytes, Bytes, Bytes32, Nonce};
use crate::types::{Block, Transaction};
use crate::{Error, Result};
use deadpool_postgres::Pool;

pub struct DbHandle {
    pool: Pool,
}

impl DbHandle {
    pub async fn new(reset_data: bool, cfg: &DbConfig) -> Result<Self> {
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

        if reset_data {
            log::info!("resetting database");

            reset_db(&conn).await;
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

    pub async fn insert_blocks(&self, blocks: &[Block]) -> Result<()> {
        let mut conn = self.get_conn().await?;

        let tx = conn
            .transaction()
            .await
            .map_err(Error::CreateDbTransaction)?;

        for block in blocks.iter() {
            insert_block(&tx, block).await?;

            for transaction in block.transactions.iter() {
                insert_transaction(&tx, transaction).await?;
            }
        }

        tx.commit().await.map_err(Error::CommitDbTx)?;

        Ok(())
    }

    pub async fn get_block(&self, block_number: i64) -> Result<Option<Block>> {
        let rows = self
            .get_conn()
            .await?
            .query(
                "SELECT 
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
                from eth_block
                WHERE number = $1;",
                &[&block_number],
            )
            .await
            .map_err(Error::DbQuery)?;
        let row = match rows.get(0) {
            Some(row) => row,
            None => return Ok(None),
        };

        Ok(Some(block_from_row(row)))
    }

    pub async fn get_blocks(&self, start: i64, end: i64) -> Result<Vec<Block>> {
        let rows = self
            .get_conn()
            .await?
            .query(
                "SELECT 
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
                from eth_block
                WHERE number >= $1 AND number < $2;",
                &[&start, &end],
            )
            .await
            .map_err(Error::DbQuery)?;

        let blocks = rows.iter().map(block_from_row).collect();

        Ok(blocks)
    }

    pub async fn get_txs_of_block(&self, block_number: i64) -> Result<Vec<Transaction>> {
        let rows = self
            .get_conn()
            .await?
            .query(
                "SELECT 
                    block_hash,
                    block_number,
                    source,
                    gas,
                    gas_price,
                    hash,
                    input,
                    nonce,
                    dest,
                    transaction_index,
                    value,
                    kind,
                    chain_id,
                    v,
                    r,
                    s
                from eth_tx
                WHERE block_number = $1;",
                &[&block_number],
            )
            .await
            .map_err(Error::DbQuery)?;

        let transactions = rows.iter().map(transaction_from_row).collect();

        Ok(transactions)
    }

    pub async fn delete_blocks_up_to(&self, block_number: i64) -> Result<()> {
        self.get_conn()
            .await?
            .execute("DELETE FROM eth_block WHERE number < $1", &[&block_number])
            .await
            .map_err(Error::DbQuery)?;
        Ok(())
    }
}

async fn insert_block<'a>(tx: &tokio_postgres::Transaction<'a>, block: &Block) -> Result<()> {
    tx.execute(
        "INSERT INTO eth_block (
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
                    $17
                );",
        &[
            &*block.number,
            &block.hash.as_slice(),
            &block.parent_hash.as_slice(),
            &block.nonce.0.to_be_bytes().as_slice(),
            &block.sha3_uncles.as_slice(),
            &block.logs_bloom.as_slice(),
            &block.transactions_root.as_slice(),
            &block.state_root.as_slice(),
            &block.receipts_root.as_slice(),
            &block.miner.as_slice(),
            &block.difficulty.as_slice(),
            &block.total_difficulty.as_slice(),
            &block.extra_data.as_slice(),
            &*block.size,
            &block.gas_limit.as_slice(),
            &block.gas_used.as_slice(),
            &*block.timestamp,
        ],
    )
    .await
    .map_err(Error::InsertBlock)?;

    Ok(())
}

async fn insert_transaction<'a>(
    tx: &tokio_postgres::Transaction<'a>,
    transaction: &Transaction,
) -> Result<()> {
    tx.execute(
        "INSERT INTO eth_tx (
                    block_hash,
                    block_number,
                    source,
                    gas,
                    gas_price,
                    hash,
                    input,
                    nonce,
                    dest,
                    transaction_index,
                    value,
                    kind,
                    chain_id,
                    v,
                    r,
                    s
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
                    $16
                );",
        &[
            &transaction.block_hash.as_slice(),
            &*transaction.block_number,
            &transaction.source.as_slice(),
            &*transaction.gas,
            &*transaction.gas_price,
            &transaction.hash.as_slice(),
            &transaction.input.as_slice(),
            &transaction.nonce.0.to_be_bytes().as_slice(),
            &transaction.dest.as_ref().map(|to| to.as_slice()),
            &*transaction.transaction_index,
            &transaction.value.as_slice(),
            &*transaction.kind,
            &*transaction.chain_id,
            &*transaction.v,
            &transaction.r.as_slice(),
            &transaction.s.as_slice(),
        ],
    )
    .await
    .map_err(Error::InsertTransaction)?;

    Ok(())
}

fn block_from_row(row: &tokio_postgres::Row) -> Block {
    Block {
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
    }
}

fn transaction_from_row(row: &tokio_postgres::Row) -> Transaction {
    Transaction {
        block_hash: Bytes32::new(row.get(0)),
        block_number: BigInt(row.get(1)),
        source: Address::new(row.get(2)),
        gas: BigInt(row.get(3)),
        gas_price: BigInt(row.get(4)),
        hash: Bytes32::new(row.get(5)),
        input: Bytes(row.get(6)),
        nonce: Nonce::new(row.get(7)),
        dest: row.get::<_, Option<_>>(8).map(Address::new),
        transaction_index: BigInt(row.get(9)),
        value: Bytes(row.get(10)),
        kind: BigInt(row.get(11)),
        chain_id: BigInt(row.get(12)),
        v: BigInt(row.get(13)),
        r: Bytes(row.get(14)),
        s: Bytes(row.get(15)),
    }
}

async fn reset_db(conn: &deadpool_postgres::Object) {
    let res = conn
        .execute(
            "
        DROP TABLE eth_log;
    ",
            &[],
        )
        .await;

    if let Err(e) = res {
        log::warn!("failed to delete log table {}", e);
    }

    let res = conn
        .execute(
            "
        DROP TABLE eth_tx;
    ",
            &[],
        )
        .await;

    if let Err(e) = res {
        log::warn!("failed to delete transaction table {}", e);
    }

    let res = conn
        .execute(
            "
        DROP TABLE eth_block;
    ",
            &[],
        )
        .await;

    if let Err(e) = res {
        log::warn!("failed to delete block table {}", e);
    }
}

async fn init_db(conn: &deadpool_postgres::Object) -> Result<()> {
    conn.batch_execute(
        "
        CREATE TABLE IF NOT EXISTS eth_block (
            number bigint primary key,
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
            timestamp bigint
        );
        
        CREATE TABLE IF NOT EXISTS eth_tx (
            row_id BIGSERIAL PRIMARY KEY,
            block_hash bytea,
            block_number bigint REFERENCES eth_block(number) ON DELETE CASCADE,
            source bytea,
            gas bigint,
            gas_price bigint,
            hash bytea,
            input bytea,
            nonce bytea,
            dest bytea,
            transaction_index bigint,
            value bytea,
            kind bigint,
            chain_id bigint,
            v bigint,
            r bytea,
            s bytea
        );

        CREATE INDEX IF NOT EXISTS tx_bn_idx ON eth_tx USING btree (block_number);
        
        CREATE TABLE IF NOT EXISTS eth_log (
            row_id BIGSERIAL PRIMARY KEY,
            removed boolean,
            log_index bytea,
            transaction_index bytea,
            transaction_hash bytea,
            block_hash bytea,
            block_number bigint REFERENCES eth_block(number) ON DELETE CASCADE,
            address bytea,
            data bytea,
            topic0 bytea,
            topic1 bytea,
            topic2 bytea,
            topic3 bytea
        );

        CREATE INDEX IF NOT EXISTS log_bn_idx ON eth_log USING btree (block_number);
    ",
    )
    .await
    .map_err(Error::InitDb)?;

    Ok(())
}
