use parquet::basic::Type as BasicType;
use parquet::basic::{Compression, ConvertedType, Repetition};
use parquet::file::properties::{WriterProperties, WriterPropertiesPtr, WriterVersion};
use parquet::schema::types::{Type, TypePtr};
use std::sync::Arc;

pub fn block_schema() -> TypePtr {
    let schema = Type::group_type_builder("block")
        .with_fields(&mut vec![
            Arc::new(
                Type::primitive_type_builder("number", BasicType::INT64)
                    .with_repetition(Repetition::OPTIONAL)
                    .build()
                    .unwrap(),
            ),
            Arc::new(
                Type::primitive_type_builder("hash", BasicType::FIXED_LEN_BYTE_ARRAY)
                    .with_repetition(Repetition::OPTIONAL)
                    .with_length(32)
                    .build()
                    .unwrap(),
            ),
            Arc::new(
                Type::primitive_type_builder("parent_hash", BasicType::FIXED_LEN_BYTE_ARRAY)
                    .with_repetition(Repetition::REQUIRED)
                    .with_length(32)
                    .build()
                    .unwrap(),
            ),
            Arc::new(
                Type::primitive_type_builder("uncles_hash", BasicType::FIXED_LEN_BYTE_ARRAY)
                    .with_repetition(Repetition::REQUIRED)
                    .with_length(32)
                    .build()
                    .unwrap(),
            ),
            Arc::new(
                Type::primitive_type_builder("author", BasicType::FIXED_LEN_BYTE_ARRAY)
                    .with_repetition(Repetition::REQUIRED)
                    .with_length(20)
                    .build()
                    .unwrap(),
            ),
            Arc::new(
                Type::primitive_type_builder("timestamp", BasicType::FIXED_LEN_BYTE_ARRAY)
                    .with_repetition(Repetition::REQUIRED)
                    .with_length(32)
                    .build()
                    .unwrap(),
            ),
            Arc::new(
                Type::primitive_type_builder("size", BasicType::FIXED_LEN_BYTE_ARRAY)
                    .with_repetition(Repetition::OPTIONAL)
                    .with_length(32)
                    .build()
                    .unwrap(),
            ),
            Arc::new(
                Type::primitive_type_builder("nonce", BasicType::FIXED_LEN_BYTE_ARRAY)
                    .with_repetition(Repetition::OPTIONAL)
                    .with_length(32)
                    .build()
                    .unwrap(),
            ),
        ])
        .build()
        .unwrap();

    Arc::new(schema)
}

pub fn tx_schema() -> TypePtr {
    let schema = Type::group_type_builder("transaction")
        .with_fields(&mut vec![
            Arc::new(
                Type::primitive_type_builder("block_number", BasicType::INT64)
                    .with_repetition(Repetition::OPTIONAL)
                    .build()
                    .unwrap(),
            ),
            Arc::new(
                Type::primitive_type_builder("transaction_index", BasicType::INT64)
                    .with_repetition(Repetition::OPTIONAL)
                    .build()
                    .unwrap(),
            ),
            Arc::new(
                Type::primitive_type_builder("hash", BasicType::FIXED_LEN_BYTE_ARRAY)
                    .with_repetition(Repetition::REQUIRED)
                    .with_length(32)
                    .build()
                    .unwrap(),
            ),
            Arc::new(
                Type::primitive_type_builder("nonce", BasicType::FIXED_LEN_BYTE_ARRAY)
                    .with_repetition(Repetition::REQUIRED)
                    .with_length(32)
                    .build()
                    .unwrap(),
            ),
            Arc::new(
                Type::primitive_type_builder("block_hash", BasicType::FIXED_LEN_BYTE_ARRAY)
                    .with_repetition(Repetition::OPTIONAL)
                    .with_length(32)
                    .build()
                    .unwrap(),
            ),
            Arc::new(
                Type::primitive_type_builder("from", BasicType::FIXED_LEN_BYTE_ARRAY)
                    .with_repetition(Repetition::OPTIONAL)
                    .with_length(20)
                    .build()
                    .unwrap(),
            ),
            Arc::new(
                Type::primitive_type_builder("to", BasicType::FIXED_LEN_BYTE_ARRAY)
                    .with_repetition(Repetition::OPTIONAL)
                    .with_length(20)
                    .build()
                    .unwrap(),
            ),
            Arc::new(
                Type::primitive_type_builder("value", BasicType::FIXED_LEN_BYTE_ARRAY)
                    .with_repetition(Repetition::REQUIRED)
                    .with_length(32)
                    .build()
                    .unwrap(),
            ),
            Arc::new(
                Type::primitive_type_builder("input", BasicType::BYTE_ARRAY)
                    .with_repetition(Repetition::REQUIRED)
                    .build()
                    .unwrap(),
            ),
            Arc::new(
                Type::primitive_type_builder("transaction_type", BasicType::INT64)
                    .with_repetition(Repetition::OPTIONAL)
                    .build()
                    .unwrap(),
            ),
        ])
        .build()
        .unwrap();

    Arc::new(schema)
}

pub fn log_schema() -> TypePtr {
    let schema = Type::group_type_builder("log")
        .with_fields(&mut vec![
            Arc::new(
                Type::primitive_type_builder("block_number", BasicType::INT64)
                    .with_repetition(Repetition::OPTIONAL)
                    .build()
                    .unwrap(),
            ),
            Arc::new(
                Type::primitive_type_builder("address", BasicType::FIXED_LEN_BYTE_ARRAY)
                    .with_repetition(Repetition::REQUIRED)
                    .with_length(20)
                    .build()
                    .unwrap(),
            ),
            Arc::new(
                Type::primitive_type_builder("block_hash", BasicType::FIXED_LEN_BYTE_ARRAY)
                    .with_repetition(Repetition::OPTIONAL)
                    .with_length(32)
                    .build()
                    .unwrap(),
            ),
            Arc::new(
                Type::primitive_type_builder("transaction_index", BasicType::INT64)
                    .with_repetition(Repetition::OPTIONAL)
                    .build()
                    .unwrap(),
            ),
            Arc::new(
                Type::primitive_type_builder("transaction_hash", BasicType::FIXED_LEN_BYTE_ARRAY)
                    .with_repetition(Repetition::OPTIONAL)
                    .with_length(32)
                    .build()
                    .unwrap(),
            ),
            Arc::new(
                Type::primitive_type_builder("log_index", BasicType::FIXED_LEN_BYTE_ARRAY)
                    .with_repetition(Repetition::OPTIONAL)
                    .with_length(32)
                    .build()
                    .unwrap(),
            ),
            Arc::new(
                Type::primitive_type_builder(
                    "transaction_log_index",
                    BasicType::FIXED_LEN_BYTE_ARRAY,
                )
                .with_repetition(Repetition::OPTIONAL)
                .with_length(32)
                .build()
                .unwrap(),
            ),
            Arc::new(
                Type::primitive_type_builder("transaction_log_index", BasicType::BYTE_ARRAY)
                    .with_repetition(Repetition::OPTIONAL)
                    .with_converted_type(ConvertedType::UTF8)
                    .build()
                    .unwrap(),
            ),
        ])
        .build()
        .unwrap();

    Arc::new(schema)
}

pub fn writer_properties() -> WriterPropertiesPtr {
    let props = WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_compression(Compression::LZ4)
        .build();

    Arc::new(props)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_block_schema() {
        block_schema();
    }

    #[test]
    fn test_tx_schema() {
        tx_schema();
    }

    #[test]
    fn test_log_schema() {
        log_schema();
    }

    #[test]
    fn test_writer_properties() {
        writer_properties();
    }
}
