use crate::config::DbConfig;
use crate::deserialize::{Address, BigInt, BloomFilterBytes, Bytes, Bytes32, Nonce};
use crate::types::{Block, Log, Transaction};
use crate::{Error, Result};
use deadpool_postgres::Pool;
use serde_json::Value as JsonValue;
use tokio_postgres::types::ToSql;

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

        log::info!("initializing database schema");

        init_db(&conn).await?;

        Ok(Self { pool })
    }

    async fn get_conn(&self) -> Result<deadpool_postgres::Object> {
        self.pool.get().await.map_err(Error::GetDbConnection)
    }

    pub async fn raw_query_to_json(
        &self,
        query: &str,
    ) -> Result<Vec<serde_json::Map<String, JsonValue>>> {
        todo!()
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

    pub async fn insert_blocks(&self, blocks: &[Block], logs: &[Log]) -> Result<()> {
        let mut conn = self.get_conn().await?;

        let tx = conn
            .transaction()
            .await
            .map_err(Error::CreateDbTransaction)?;

        let mut block_params: Vec<&(dyn ToSql + Sync)> = Vec::with_capacity(blocks.len() * 17);

        for block in blocks.iter() {
            block_params.extend_from_slice(&[
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
            ]);
        }

        let block_query = format!(
            "
            INSERT INTO eth_block (
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
            ) {};
        ",
            (1..blocks.len())
                .map(|i| {
                    let i = i * 17;

                    format!(
                        ", (
                ${},
                ${},
                ${},
                ${},
                ${},
                ${},
                ${},
                ${},
                ${},
                ${},
                ${},
                ${},
                ${},
                ${},
                ${},
                ${},
                ${}
            )",
                        i + 1,
                        i + 2,
                        i + 3,
                        i + 4,
                        i + 5,
                        i + 6,
                        i + 7,
                        i + 8,
                        i + 9,
                        i + 10,
                        i + 11,
                        i + 12,
                        i + 13,
                        i + 14,
                        i + 15,
                        i + 16,
                        i + 17
                    )
                })
                .fold(String::new(), |a, b| a + &b)
        );

        tx.execute(&block_query, &block_params)
            .await
            .map_err(Error::InsertBlocks)?;

        let transactions = blocks
            .iter()
            .flat_map(|b| b.transactions.iter())
            .collect::<Vec<_>>();

        if !transactions.is_empty() {
            for chunk in transactions.chunks(i16::MAX as usize / 16) {
                let transaction_query = format!(
                    "
                INSERT INTO eth_tx (
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
                    ) {};
        ",
                    (1..chunk.len())
                        .map(|i| {
                            let i = i * 16;

                            format!(
                                ", (
                            ${},
                            ${},
                            ${},
                            ${},
                            ${},
                            ${},
                            ${},
                            ${},
                            ${},
                            ${},
                            ${},
                            ${},
                            ${},
                            ${},
                            ${},
                            ${}
                        )",
                                i + 1,
                                i + 2,
                                i + 3,
                                i + 4,
                                i + 5,
                                i + 6,
                                i + 7,
                                i + 8,
                                i + 9,
                                i + 10,
                                i + 11,
                                i + 12,
                                i + 13,
                                i + 14,
                                i + 15,
                                i + 16
                            )
                        })
                        .fold(String::new(), |a, b| a + &b)
                );

                let transaction_params = chunk
                    .iter()
                    .map(|transaction| -> [&(dyn ToSql + Sync); 16] {
                        [
                            &transaction.block_hash,
                            &transaction.block_number,
                            &transaction.source,
                            &transaction.gas,
                            &transaction.gas_price,
                            &transaction.hash,
                            &transaction.input,
                            &transaction.nonce,
                            &transaction.dest,
                            &transaction.transaction_index,
                            &transaction.value,
                            &transaction.kind,
                            &transaction.chain_id,
                            &transaction.v,
                            &transaction.r,
                            &transaction.s,
                        ]
                    })
                    .fold(Vec::with_capacity(chunk.len() * 16), |mut a, b| {
                        a.extend_from_slice(&b);
                        a
                    });

                tx.execute(&transaction_query, &transaction_params)
                    .await
                    .map_err(Error::InsertTransactions)?;
            }
        }

        if !logs.is_empty() {
            for chunk in logs.chunks(i16::MAX as usize / 9) {
                let log_query = format!(
                    "
            INSERT INTO eth_log (
                    address,
                    block_hash,
                    block_number,
                    data,
                    log_index,
                    removed,
                    topics,
                    transaction_hash,
                    transaction_index
                ) VALUES (
                    $1,
                    $2,
                    $3,
                    $4,
                    $5,
                    $6,
                    $7,
                    $8,
                    $9
                ) {};
    ",
                    (1..chunk.len())
                        .map(|i| {
                            let i = i * 9;

                            format!(
                                ", (
                        ${},
                        ${},
                        ${},
                        ${},
                        ${},
                        ${},
                        ${},
                        ${},
                        ${}
                    )",
                                i + 1,
                                i + 2,
                                i + 3,
                                i + 4,
                                i + 5,
                                i + 6,
                                i + 7,
                                i + 8,
                                i + 9,
                            )
                        })
                        .fold(String::new(), |a, b| a + &b)
                );

                let log_params = chunk
                    .iter()
                    .map(|log| -> [&(dyn ToSql + Sync); 9] {
                        [
                            &log.address,
                            &log.block_hash,
                            &log.block_number,
                            &log.data,
                            &log.log_index,
                            &log.removed,
                            &log.topics,
                            &log.transaction_hash,
                            &log.transaction_index,
                        ]
                    })
                    .fold(Vec::with_capacity(chunk.len() * 9), |mut a, b| {
                        a.extend_from_slice(&b);
                        a
                    });

                tx.execute(&log_query, &log_params)
                    .await
                    .map_err(Error::InsertLogs)?;
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

    pub async fn get_logs(&self, from: i64, to: i64) -> Result<Vec<Log>> {
        let rows = self
            .get_conn()
            .await?
            .query(
                "
            SELECT
                address,
                block_hash,
                block_number,
                data,
                log_index,
                removed,
                topics,
                transaction_hash,
                transaction_index
            from eth_log
            WHERE block_number >= $1 AND block_number < $2;
        ",
                &[&from, &to],
            )
            .await
            .map_err(Error::DbQuery)?;

        let logs = rows.iter().map(log_from_row).collect();

        Ok(logs)
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

fn log_from_row(row: &tokio_postgres::Row) -> Log {
    Log {
        address: Address::new(row.get(0)),
        block_hash: Bytes32::new(row.get(1)),
        block_number: BigInt(row.get(2)),
        data: Bytes(row.get(3)),
        log_index: BigInt(row.get(4)),
        removed: row.get(5),
        topics: row
            .get::<_, Vec<Vec<u8>>>(6)
            .into_iter()
            .map(Bytes32::new)
            .collect(),
        transaction_hash: Bytes32::new(row.get(7)),
        transaction_index: BigInt(row.get(8)),
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
            number bigint primary key NOT NULL,
            hash bytea NOT NULL,
            parent_hash bytea NOT NULL,
            nonce bytea NOT NULL,
            sha3_uncles bytea NOT NULL,
            logs_bloom bytea NOT NULL,
            transactions_root bytea NOT NULL,
            state_root bytea NOT NULL,
            receipts_root bytea NOT NULL,
            miner bytea NOT NULL,
            difficulty bytea NOT NULL,
            total_difficulty bytea NOT NULL,
            extra_data bytea NOT NULL,
            size bigint NOT NULL,
            gas_limit bytea NOT NULL,
            gas_used bytea NOT NULL,
            timestamp bigint NOT NULL
        );
        
        CREATE TABLE IF NOT EXISTS eth_tx (
            row_id BIGSERIAL PRIMARY KEY NOT NULL,
            block_hash bytea NOT NULL,
            block_number bigint NOT NULL REFERENCES eth_block(number) ON DELETE CASCADE,
            source bytea NOT NULL,
            gas bigint NOT NULL,
            gas_price bigint NOT NULL,
            hash bytea NOT NULL,
            input bytea NOT NULL,
            nonce bytea NOT NULL,
            dest bytea,
            transaction_index bigint NOT NULL,
            value bytea NOT NULL,
            kind bigint NOT NULL,
            chain_id bigint NOT NULL,
            v bigint NOT NULL,
            r bytea NOT NULL,
            s bytea NOT NULL
        );

        CREATE INDEX IF NOT EXISTS tx_bn_idx ON eth_tx USING btree (block_number);
        CREATE INDEX IF NOT EXISTS tx_idx_idx ON eth_tx USING btree (block_number, transaction_index);

        CREATE TABLE IF NOT EXISTS eth_log (
            row_id BIGSERIAL PRIMARY KEY NOT NULL,
            address bytea NOT NULL,
            block_hash bytea NOT NULL,
            block_number bigint NOT NULL REFERENCES eth_block(number) ON DELETE CASCADE,
            data bytea NOT NULL,
            log_index bigint NOT NULL,
            removed boolean NOT NULL,
            topics bytea[],
            transaction_hash bytea NOT NULL,
            transaction_index bigint NOT NULL
        );

        CREATE INDEX IF NOT EXISTS log_bn_idx ON eth_log USING btree (block_number);
        CREATE INDEX IF NOT EXISTS log_addr_idx ON eth_log USING btree (block_number, address);
    ",
    )
    .await
    .map_err(Error::InitDb)?;

    Ok(())
}
