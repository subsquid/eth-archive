use super::util::{define_cols, map_from_arrow, map_from_arrow_opt};
use super::ParquetQuery;
use crate::parquet_metadata::BlockRowGroupMetadata;
use crate::types::MiniQuery;
use crate::{Error, Result};
use arrow2::array::{self, Array, UInt32Array, UInt64Array};
use arrow2::io::parquet;
use eth_archive_core::deserialize::{
    Address, BigUnsigned, BloomFilterBytes, Bytes, Bytes32, Index,
};
use eth_archive_core::types::ResponseBlock;
use eth_archive_ingester::schema::log_schema;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::{fs, io};

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

pub fn query_blocks(
    query: Arc<ParquetQuery>,
    pruned_blocks_per_rg: Vec<Option<BTreeSet<u32>>>,
) -> Result<BTreeMap<u32, ResponseBlock>> {
    let mut path = query.data_path.clone();
    path.push(query.dir_name.to_string());
    path.push("block.parquet");
    let file = fs::File::open(&path).map_err(Error::OpenParquetFile)?;
    let mut reader = io::BufReader::new(file);

    let metadata = parquet::read::read_metadata(&mut reader).map_err(Error::ReadParquet)?;

    let selected_fields = query.mini_query.field_selection.block.as_fields();

    let fields: Vec<_> = log_schema()
        .fields
        .into_iter()
        .filter(|field| selected_fields.contains(field.name.as_str()))
        .collect();

    let mut blocks = BTreeMap::new();

    for (rg_meta, block_nums) in metadata.row_groups.iter().zip(pruned_blocks_per_rg.iter()) {
        if let Some(block_nums) = block_nums {
            if block_nums.is_empty() {
                continue;
            }
        }

        let columns = parquet::read::read_columns_many(
            &mut reader,
            rg_meta,
            fields.clone(),
            None,
            None,
            None,
        )
        .map_err(Error::ReadParquet)?;

        for columns in columns {
            let columns = columns
                .into_iter()
                .zip(fields.iter())
                .map(|(col, field)| {
                    let col = col.map_err(Error::ReadParquet)?;
                    Ok((field.name.to_owned(), col))
                })
                .collect::<Result<HashMap<_, _>>>()?;

            process_cols(&query.mini_query, block_nums, columns, &mut blocks);
        }
    }

    Ok(blocks)
}

fn process_cols(
    query: &MiniQuery,
    block_nums: &Option<BTreeSet<u32>>,
    columns: HashMap<String, Box<dyn Array>>,
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
