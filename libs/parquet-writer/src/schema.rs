use crate::error::Result;
use arrow2::array::Array;
use arrow2::chunk::Chunk as ArrowChunk;
use arrow2::datatypes::{DataType, Schema};
use arrow2::error::Error as ArrowError;
use arrow2::io::parquet::write::{transverse, Encoding, RowGroupIterator, WriteOptions};
use arrow2::io::parquet::write::{CompressionOptions, Version};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use std::result::Result as StdResult;
use std::vec::IntoIter;

pub type Chunk = ArrowChunk<Box<dyn Array>>;
pub type RowGroups = RowGroupIterator<Box<dyn Array>, IntoIter<StdResult<Chunk, ArrowError>>>;

fn options() -> WriteOptions {
    WriteOptions {
        write_statistics: true,
        compression: CompressionOptions::Lz4Raw,
        version: Version::V2,
    }
}

pub trait IntoRowGroups: Default + std::marker::Sized + Send + Sync {
    type Elem: Send + Sync + std::fmt::Debug + 'static + std::marker::Sized;

    fn schema() -> Schema;
    fn into_chunk(self) -> Chunk;
    fn into_row_groups(elems: Vec<Self>) -> (RowGroups, Schema, WriteOptions) {
        let schema = Self::schema();

        let encoding_map = |data_type: &DataType| match data_type {
            DataType::Binary | DataType::LargeBinary => Encoding::DeltaLengthByteArray,
            _ => Encoding::Plain,
        };

        let encodings = schema
            .fields
            .iter()
            .map(|f| transverse(&f.data_type, encoding_map))
            .collect::<Vec<_>>();

        let row_groups = RowGroupIterator::try_new(
            elems
                .into_par_iter()
                .map(|elem| Ok(Self::into_chunk(elem)))
                .collect::<Vec<_>>()
                .into_iter(),
            &schema,
            options(),
            encodings,
        )
        .unwrap();

        (row_groups, schema, options())
    }
    fn push(&mut self, elem: Self::Elem) -> Result<()>;
    fn block_num(&self, elem: &Self::Elem) -> u32;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
