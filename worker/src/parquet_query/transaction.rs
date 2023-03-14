use super::read::ReadParquet;
use super::util::{define_cols, map_from_arrow, map_from_arrow_opt};
use super::ParquetQuery;
use crate::parquet_metadata::{combine_block_num_tx_idx, TransactionRowGroupMetadata};
use crate::types::{MiniQuery, MiniTransactionSelection};
use crate::Result;
use arrow2::array::{self, Array, UInt32Array, UInt64Array};
use eth_archive_core::deserialize::{Address, BigUnsigned, Bytes, Bytes32, Index, Sighash};
use eth_archive_core::hash::{HashMap, HashSet};
use eth_archive_core::rayon_async;
use eth_archive_core::types::ResponseTransaction;
use eth_archive_ingester::schema::tx_schema;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

type BinaryArray = array::BinaryArray<i32>;

pub fn prune_tx_queries_per_rg(
    rg_meta: &TransactionRowGroupMetadata,
    tx_selections: &[MiniTransactionSelection],
    transactions: BTreeSet<(u32, u32)>,
) -> (Vec<MiniTransactionSelection>, BTreeSet<(u32, u32)>) {
    let transactions = transactions
        .into_iter()
        .filter(|(block_num, tx_idx)| {
            let block_num_tx_idx = combine_block_num_tx_idx(*block_num, *tx_idx);
            rg_meta.max_blk_num_tx_idx >= block_num_tx_idx
                && rg_meta.min_blk_num_tx_idx <= block_num_tx_idx
        })
        .collect();

    let tx_selections = tx_selections
        .iter()
        .filter_map(|tx_selection| {
            if tx_selection.source.is_empty() && tx_selection.dest.is_empty() {
                return Some(tx_selection.clone());
            }

            let source = tx_selection
                .source
                .iter()
                .filter(|addr| rg_meta.source_filter.contains(addr))
                .cloned()
                .collect::<HashSet<_>>();

            if source.is_empty() {
                return None;
            }

            let dest = tx_selection
                .dest
                .iter()
                .filter(|addr| rg_meta.dest_filter.contains(addr))
                .cloned()
                .collect::<HashSet<_>>();

            if dest.is_empty() {
                return None;
            }

            Some(MiniTransactionSelection {
                source,
                dest,
                sighash: tx_selection.sighash.clone(),
                status: tx_selection.status,
            })
        })
        .collect();

    (tx_selections, transactions)
}

type TxIds = BTreeSet<(u32, u32)>;
type Txs = BTreeMap<(u32, u32), ResponseTransaction>;

pub async fn query_transactions(
    query: Arc<ParquetQuery>,
    pruned_queries_per_rg: Vec<(Vec<MiniTransactionSelection>, TxIds)>,
    blocks: BTreeSet<u32>,
) -> Result<(Txs, BTreeSet<u32>)> {
    let mut path = query.data_path.clone();
    path.push(query.dir_name.to_string());
    path.push("tx.parquet");

    let selected_fields = query.mini_query.field_selection.transaction.as_fields();

    let fields: Vec<_> = tx_schema()
        .fields
        .into_iter()
        .filter(|field| selected_fields.contains(field.name.as_str()))
        .collect();

    let rg_filter = |i| {
        let (tx_queries, tx_ids): &(Vec<MiniTransactionSelection>, TxIds) =
            &pruned_queries_per_rg[i];
        !tx_queries.is_empty() || !tx_ids.is_empty()
    };

    let chunk_rx = ReadParquet {
        path,
        rg_filter,
        fields,
    }
    .read()
    .await?;

    rayon_async::spawn(move || {
        let mut blocks = blocks;
        let mut transactions = BTreeMap::new();
        while let Ok(res) = chunk_rx.recv() {
            let (i, columns) = res?;
            let queries = &pruned_queries_per_rg[i];
            process_cols(
                &query.mini_query,
                &queries.0,
                &queries.1,
                columns,
                &mut blocks,
                &mut transactions,
            );
        }

        Ok((transactions, blocks))
    })
    .await
}

fn process_cols(
    query: &MiniQuery,
    tx_queries: &[MiniTransactionSelection],
    tx_ids: &BTreeSet<(u32, u32)>,
    mut columns: HashMap<String, Box<dyn Array>>,
    blocks: &mut BTreeSet<u32>,
    transactions: &mut BTreeMap<(u32, u32), ResponseTransaction>,
) {
    #[rustfmt::skip]
	define_cols!(
    	columns,
    	kind, UInt32Array,
        nonce, UInt64Array,
        dest, BinaryArray,
        gas, BinaryArray,
        value, BinaryArray,
        input, BinaryArray,
        max_priority_fee_per_gas, BinaryArray,
        max_fee_per_gas, BinaryArray,
        y_parity, UInt32Array,
        chain_id, UInt32Array,
        v, UInt64Array,
        r, BinaryArray,
        s, BinaryArray,
        source, BinaryArray,
        block_hash, BinaryArray,
        block_number, UInt32Array,
        transaction_index, UInt32Array,
        gas_price, BinaryArray,
        hash, BinaryArray,
        status, UInt32Array,
        sighash, BinaryArray
	);

    let len = block_number.as_ref().unwrap().len();

    for i in 0..len {
        let tx = ResponseTransaction {
            kind: map_from_arrow_opt!(kind, Index, i),
            nonce: map_from_arrow!(nonce, BigUnsigned, i),
            dest: map_from_arrow_opt!(dest, Address::new, i),
            gas: map_from_arrow!(gas, Bytes::new, i),
            value: map_from_arrow!(value, Bytes::new, i),
            input: map_from_arrow!(input, Bytes::new, i),
            max_priority_fee_per_gas: map_from_arrow_opt!(max_priority_fee_per_gas, Bytes::new, i),
            max_fee_per_gas: map_from_arrow_opt!(max_fee_per_gas, Bytes::new, i),
            y_parity: map_from_arrow_opt!(y_parity, Index, i),
            chain_id: map_from_arrow_opt!(chain_id, Index, i),
            v: map_from_arrow_opt!(v, BigUnsigned, i),
            r: map_from_arrow!(r, Bytes::new, i),
            s: map_from_arrow!(s, Bytes::new, i),
            source: map_from_arrow_opt!(source, Address::new, i),
            block_hash: map_from_arrow!(block_hash, Bytes32::new, i),
            block_number: map_from_arrow!(block_number, Index, i),
            transaction_index: map_from_arrow!(transaction_index, Index, i),
            gas_price: map_from_arrow!(gas_price, Bytes::new, i),
            hash: map_from_arrow!(hash, Bytes32::new, i),
            status: map_from_arrow_opt!(status, Index, i),
        };

        let sighash = map_from_arrow!(sighash, Sighash::new, i);

        let block_number = tx.block_number.unwrap().0;
        let transaction_index = tx.transaction_index.unwrap().0;

        if query.from_block > block_number || query.to_block <= block_number {
            continue;
        }

        let tx_id = (block_number, transaction_index);
        if !tx_ids.contains(&tx_id)
            && !MiniTransactionSelection::matches_tx_impl(
                tx_queries, &tx.source, &tx.dest, &sighash, tx.status,
            )
        {
            continue;
        }

        blocks.insert(block_number);
        transactions.insert(tx_id, tx);
    }
}
