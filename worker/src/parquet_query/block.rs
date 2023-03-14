use super::read::ReadParquet;
use super::util::{define_cols, map_from_arrow, map_from_arrow_opt};
use super::ParquetQuery;
use crate::parquet_metadata::BlockRowGroupMetadata;
use crate::types::MiniQuery;
use crate::Result;
use arrow2::array::{self, Array, UInt32Array, UInt64Array};
use eth_archive_core::deserialize::{
    Address, BigUnsigned, BloomFilterBytes, Bytes, Bytes32, Index,
};
use eth_archive_core::hash::HashMap;
use eth_archive_core::types::ResponseBlock;
use eth_archive_ingester::schema::block_schema;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

type BinaryArray = array::BinaryArray<i32>;

pub fn prune_blocks_per_rg(
    rg_meta: &BlockRowGroupMetadata,
    blocks: &Option<BTreeSet<u32>>,
) -> Option<BTreeSet<u32>> {
    blocks.as_ref().map(|blocks| {
        blocks
            .iter()
            .filter_map(|&blk_num| {
                if rg_meta.max_block_number < blk_num || rg_meta.min_block_number > blk_num {
                    None
                } else {
                    Some(blk_num)
                }
            })
            .collect()
    })
}

pub async fn query_blocks(
    query: Arc<ParquetQuery>,
    pruned_blocks_per_rg: Vec<Option<BTreeSet<u32>>>,
) -> Result<BTreeMap<u32, ResponseBlock>> {
    let mut path = query.data_path.clone();
    path.push(query.dir_name.to_string());
    path.push("block.parquet");

    let selected_fields = query.mini_query.field_selection.block.as_fields();

    let fields: Vec<_> = block_schema()
        .fields
        .into_iter()
        .filter(|field| selected_fields.contains(field.name.as_str()))
        .collect();

    let rg_filter = |i| {
        let val: &Option<BTreeSet<u32>> = &pruned_blocks_per_rg[i];
        if let Some(block_nums) = val {
            !block_nums.is_empty()
        } else {
            true
        }
    };

    let chunk_rx = ReadParquet {
        path,
        rg_filter,
        fields,
    }
    .read()
    .await?;

    tokio::task::spawn_blocking(move || {
        let mut blocks = BTreeMap::new();
        while let Ok(res) = chunk_rx.recv() {
            let (i, columns) = res?;
            let block_nums = &pruned_blocks_per_rg[i];
            process_cols(&query.mini_query, block_nums, columns, &mut blocks);
        }

        Ok(blocks)
    })
    .await
    .unwrap()
}

fn process_cols(
    query: &MiniQuery,
    block_nums: &Option<BTreeSet<u32>>,
    mut columns: HashMap<String, Box<dyn Array>>,
    blocks: &mut BTreeMap<u32, ResponseBlock>,
) {
    #[rustfmt::skip]
	define_cols!(
    	columns,
        number, UInt32Array,
        parent_hash, BinaryArray,
        sha3_uncles, BinaryArray,
        miner, BinaryArray,
        state_root, BinaryArray,
        transactions_root, BinaryArray,
        receipts_root, BinaryArray,
        logs_bloom, BinaryArray,
        difficulty, BinaryArray,
        gas_limit, BinaryArray,
        gas_used, BinaryArray,
        timestamp, BinaryArray,
        extra_data, BinaryArray,
        mix_hash, BinaryArray,
        nonce, UInt64Array,
        total_difficulty, BinaryArray,
        base_fee_per_gas, BinaryArray,
        size, BinaryArray,
        hash, BinaryArray
	);

    let len = number.as_ref().unwrap().len();

    for i in 0..len {
        let block = ResponseBlock {
            parent_hash: map_from_arrow!(parent_hash, Bytes32::new, i),
            sha3_uncles: map_from_arrow!(sha3_uncles, Bytes32::new, i),
            miner: map_from_arrow!(miner, Address::new, i),
            state_root: map_from_arrow!(state_root, Bytes32::new, i),
            transactions_root: map_from_arrow!(transactions_root, Bytes32::new, i),
            receipts_root: map_from_arrow!(receipts_root, Bytes32::new, i),
            logs_bloom: map_from_arrow!(logs_bloom, BloomFilterBytes::new, i),
            difficulty: map_from_arrow_opt!(difficulty, Bytes::new, i),
            number: map_from_arrow!(number, Index, i),
            gas_limit: map_from_arrow!(gas_limit, Bytes::new, i),
            gas_used: map_from_arrow!(gas_used, Bytes::new, i),
            timestamp: map_from_arrow!(timestamp, Bytes::new, i),
            extra_data: map_from_arrow!(extra_data, Bytes::new, i),
            mix_hash: map_from_arrow_opt!(mix_hash, Bytes32::new, i),
            nonce: map_from_arrow_opt!(nonce, BigUnsigned, i),
            total_difficulty: map_from_arrow_opt!(total_difficulty, Bytes::new, i),
            base_fee_per_gas: map_from_arrow_opt!(base_fee_per_gas, Bytes::new, i),
            size: map_from_arrow!(size, Bytes::new, i),
            hash: map_from_arrow_opt!(hash, Bytes32::new, i),
        };

        let block_number = block.number.unwrap().0;

        if query.from_block > block_number || query.to_block <= block_number {
            continue;
        }

        if let Some(block_nums) = block_nums {
            if !block_nums.contains(&block_number) {
                continue;
            }
        }

        blocks.insert(block_number, block);
    }
}
