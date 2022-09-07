use crate::config::DbConfig;
use crate::deserialize::{Address, BigInt, BloomFilterBytes, Bytes, Bytes32, Nonce};
use crate::types::{
    Block, Log, QueryMetrics, QueryResult, ResponseBlock, ResponseLog, ResponseRow,
    ResponseTransaction, Transaction,
};
use crate::{Error, Result};
use deadpool_postgres::{Pool, Transaction as DbTransaction};
use std::time::Instant;
use tokio_postgres::types::ToSql;

type Params<'a> = Vec<&'a (dyn ToSql + Sync)>;

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

    pub async fn raw_query(&self, build_query: u128, query: &str) -> Result<QueryResult> {
        let start_time = Instant::now();

        let rows = self
            .get_conn()
            .await?
            .query(query, &[])
            .await
            .map_err(Error::DbQuery)?;

        let run_query = start_time.elapsed().as_millis();

        let start_time = Instant::now();

        let data = rows
            .into_iter()
            .map(|row| ResponseRow {
                block: ResponseBlock {
                    number: row.try_get("eth_block_number").ok().map(BigInt),
                    hash: row.try_get("eth_block_hash").ok().map(Bytes32::new),
                    parent_hash: row.try_get("eth_block_parent_hash").ok().map(Bytes32::new),
                    nonce: row.try_get("eth_block_nonce").ok().map(Nonce::new),
                    sha3_uncles: row.try_get("eth_block_sha3_uncles").ok().map(Bytes32::new),
                    logs_bloom: row
                        .try_get("eth_block_logs_bloom")
                        .ok()
                        .map(BloomFilterBytes::new),
                    transactions_root: row
                        .try_get("eth_block_transactions_root")
                        .ok()
                        .map(Bytes32::new),
                    state_root: row.try_get("eth_block_state_root").ok().map(Bytes32::new),
                    receipts_root: row
                        .try_get("eth_block_receipts_root")
                        .ok()
                        .map(Bytes32::new),
                    miner: row.try_get("eth_block_miner").ok().map(Address::new),
                    difficulty: row.try_get("eth_block_difficulty").ok().map(Bytes),
                    total_difficulty: row.try_get("eth_block_total_difficulty").ok().map(Bytes),
                    extra_data: row.try_get("eth_block_extra_data").ok().map(Bytes),
                    size: row.try_get("eth_block_size").ok().map(BigInt),
                    gas_limit: row.try_get("eth_block_gas_limit").ok().map(Bytes),
                    gas_used: row.try_get("eth_block_gas_used").ok().map(Bytes),
                    timestamp: row.try_get("eth_block_timestamp").ok().map(BigInt),
                },
                transaction: ResponseTransaction {
                    block_hash: row.try_get("eth_tx_block_hash").ok().map(Bytes32::new),
                    block_number: row.try_get("eth_tx_block_number").ok().map(BigInt),
                    source: row.try_get("eth_tx_source").ok().map(Address::new),
                    gas: row.try_get("eth_tx_gas").ok().map(BigInt),
                    gas_price: row.try_get("eth_tx_gas_price").ok().map(BigInt),
                    hash: row.try_get("eth_tx_hash").ok().map(Bytes32::new),
                    input: row.try_get("eth_tx_input").ok().map(Bytes::new),
                    nonce: row.try_get("eth_tx_nonce").ok().map(Nonce::new),
                    dest: match row.try_get("eth_tx_dest") {
                        Ok(Some(dest)) => Some(Address::new(dest)),
                        _ => None,
                    },
                    transaction_index: row.try_get("eth_tx_transaction_index").ok().map(BigInt),
                    value: row.try_get("eth_tx_value").ok().map(Bytes),
                    kind: row.try_get("eth_tx_kind").ok().map(BigInt),
                    chain_id: row.try_get("eth_tx_chain_id").ok().map(BigInt),
                    v: row.try_get("eth_tx_v").ok().map(BigInt),
                    r: row.try_get("eth_tx_r").ok().map(Bytes),
                    s: row.try_get("eth_tx_s").ok().map(Bytes),
                },
                log: ResponseLog {
                    address: row.try_get("eth_log_address").ok().map(Address::new),
                    block_hash: row.try_get("eth_log_block_hash").ok().map(Bytes32::new),
                    block_number: row.try_get("eth_log_block_number").ok().map(BigInt),
                    data: row.try_get("eth_log_data").ok().map(Bytes),
                    log_index: row.try_get("eth_log_log_index").ok().map(BigInt),
                    removed: row.try_get("eth_log_removed").ok(),
                    topics: {
                        let mut topics = Vec::new();

                        if let Ok(Some(topic)) = row.try_get("eth_log_topic0") {
                            topics.push(Bytes32::new(topic));
                        }

                        if let Ok(Some(topic)) = row.try_get("eth_log_topic1") {
                            topics.push(Bytes32::new(topic));
                        }

                        if let Ok(Some(topic)) = row.try_get("eth_log_topic2") {
                            topics.push(Bytes32::new(topic));
                        }

                        if let Ok(Some(topic)) = row.try_get("eth_log_topic3") {
                            topics.push(Bytes32::new(topic));
                        }

                        Some(topics)
                    },
                    transaction_hash: row
                        .try_get("eth_log_transaction_hash")
                        .ok()
                        .map(Bytes32::new),
                    transaction_index: row.try_get("eth_log_transaction_index").ok().map(BigInt),
                },
            })
            .collect();

        let serialize_result = start_time.elapsed().as_millis();

        Ok(QueryResult {
            data,
            metrics: QueryMetrics {
                build_query,
                run_query,
                serialize_result,
                total: build_query + run_query + serialize_result,
            },
        })
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
            Some(num) => Ok(Some(usize::try_from(num).unwrap())),
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
            Some(num) => Ok(Some(usize::try_from(num).unwrap())),
            None => Ok(None),
        }
    }

    pub async fn insert_blocks_data(&self, blocks: &[Block], logs: &[Log]) -> Result<()> {
        let mut conn = self.get_conn().await?;

        let tx = conn
            .transaction()
            .await
            .map_err(Error::CreateDbTransaction)?;

        self.insert_blocks(blocks, &tx).await?;

        let transactions = blocks
            .iter()
            .flat_map(|b| b.transactions.iter())
            .collect::<Vec<_>>();
        let chunk_size = usize::try_from(i16::MAX / 16).unwrap();
        for chunk in transactions.chunks(chunk_size) {
            self.insert_transactions(chunk, &tx).await?;
        }

        let chunk_size = usize::try_from(i16::MAX / 12).unwrap();
        for chunk in logs.chunks(chunk_size) {
            self.insert_logs(chunk, &tx).await?;
        }

        tx.commit().await.map_err(Error::CommitDbTx)?;

        Ok(())
    }

    async fn insert_blocks(&self, blocks: &[Block], tx: &DbTransaction<'_>) -> Result<()> {
        let columns_count = 17;
        let mut params: Params = Vec::with_capacity(blocks.len() * columns_count);
        let mut placeholders = Vec::with_capacity(blocks.len());

        for (idx, block) in blocks.iter().enumerate() {
            params.extend_from_slice(&[
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
            let start = idx * columns_count + 1;
            let mut placeholder = Vec::with_capacity(columns_count);
            for num in start..start + columns_count {
                placeholder.push(format!("${}", num));
            }
            placeholders.push(format!("({})", placeholder.join(", ")));
        }

        let query = format!(
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
            ) VALUES {}",
            &placeholders.join(", ")
        );

        tx.execute(&query, &params)
            .await
            .map_err(Error::InsertBlocks)?;
        Ok(())
    }

    async fn insert_transactions(
        &self,
        transactions: &[&Transaction],
        tx: &DbTransaction<'_>,
    ) -> Result<()> {
        let columns_count = 16;
        let mut params: Params = Vec::with_capacity(transactions.len() * columns_count);
        let mut placeholders = Vec::with_capacity(transactions.len());

        for (idx, transaction) in transactions.iter().enumerate() {
            params.extend_from_slice(&[
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
            ]);
            let start = idx * columns_count + 1;
            let mut placeholder = Vec::with_capacity(columns_count);
            for num in start..start + columns_count {
                placeholder.push(format!("${}", num));
            }
            placeholders.push(format!("({})", placeholder.join(", ")));
        }

        let query = format!(
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
            ) VALUES {}",
            &placeholders.join(", ")
        );

        tx.execute(&query, &params)
            .await
            .map_err(Error::InsertTransactions)?;

        Ok(())
    }

    async fn insert_logs(&self, logs: &[Log], tx: &DbTransaction<'_>) -> Result<()> {
        let columns_count = 12;
        let mut params: Params = Vec::with_capacity(logs.len() * columns_count);
        let mut placeholders = Vec::with_capacity(logs.len());

        let topics_count = 4;
        let mut topics = Vec::with_capacity(topics_count * logs.len());
        for log in logs.iter() {
            topics.push(log.topics.get(0));
            topics.push(log.topics.get(1));
            topics.push(log.topics.get(2));
            topics.push(log.topics.get(3));
        }

        for (idx, log) in logs.iter().enumerate() {
            let topics_index = idx * topics_count;
            params.extend_from_slice(&[
                &log.address,
                &log.block_hash,
                &log.block_number,
                &log.data,
                &log.log_index,
                &log.removed,
                &topics[topics_index],
                &topics[topics_index + 1],
                &topics[topics_index + 2],
                &topics[topics_index + 3],
                &log.transaction_hash,
                &log.transaction_index,
            ]);
            let start = idx * columns_count + 1;
            let mut placeholder = Vec::with_capacity(columns_count);
            for num in start..start + columns_count {
                placeholder.push(format!("${}", num));
            }
            placeholders.push(format!("({})", placeholder.join(", ")));
        }

        let query = format!(
            "INSERT INTO eth_log (
                address,
                block_hash,
                block_number,
                data,
                log_index,
                removed,
                topic0,
                topic1,
                topic2,
                topic3,
                transaction_hash,
                transaction_index
            ) VALUES {}",
            &placeholders.join(", ")
        );

        tx.execute(&query, &params)
            .await
            .map_err(Error::InsertLogs)?;
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

    pub async fn get_txs(&self, from: i64, to: i64) -> Result<Vec<Transaction>> {
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
                WHERE block_number >= $1 AND block_number < $2;",
                &[&from, &to],
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
                topic0,
                topic1,
                topic2,
                topic3,
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
        let mut conn = self.get_conn().await?;

        let tx = conn
            .transaction()
            .await
            .map_err(Error::CreateDbTransaction)?;

        tx.execute("DELETE FROM eth_block WHERE number < $1", &[&block_number])
            .await
            .map_err(Error::DbQuery)?;

        tx.execute(
            "DELETE FROM eth_tx WHERE block_number < $1",
            &[&block_number],
        )
        .await
        .map_err(Error::DbQuery)?;

        tx.execute(
            "DELETE FROM eth_log WHERE block_number < $1",
            &[&block_number],
        )
        .await
        .map_err(Error::DbQuery)?;

        tx.commit().await.map_err(Error::CommitDbTx)?;
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
    let mut topics = Vec::new();

    for i in 6..10 {
        if let Some(topic) = row.get(i) {
            topics.push(Bytes32::new(topic));
        }
    }

    Log {
        address: Address::new(row.get(0)),
        block_hash: Bytes32::new(row.get(1)),
        block_number: BigInt(row.get(2)),
        data: Bytes(row.get(3)),
        log_index: BigInt(row.get(4)),
        removed: row.get(5),
        topics,
        transaction_hash: Bytes32::new(row.get(10)),
        transaction_index: BigInt(row.get(11)),
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
            number bigint PRIMARY KEY NOT NULL,
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
            block_hash bytea NOT NULL,
            block_number bigint NOT NULL,
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
            s bytea NOT NULL,
            PRIMARY KEY(block_number, transaction_index)
        );

        CREATE TABLE IF NOT EXISTS eth_log (
            address bytea NOT NULL,
            block_hash bytea NOT NULL,
            block_number bigint NOT NULL,
            data bytea NOT NULL,
            log_index bigint NOT NULL,
            removed boolean NOT NULL,
            topic0 bytea,
            topic1 bytea,
            topic2 bytea,
            topic3 bytea,
            transaction_hash bytea NOT NULL,
            transaction_index bigint NOT NULL,
            PRIMARY KEY(block_number, log_index)
        );
    ",
    )
    .await
    .map_err(Error::InitDb)?;

    Ok(())
}
