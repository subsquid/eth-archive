use crate::deserialize::{BigUnsigned, Bytes};

// defines columns using the result frame and a list of column names
#[macro_export]
macro_rules! define_cols {
    ($columns:expr, $($name:ident, $arrow_type:ident),*) => {
        let mut columns = $columns.into_iter();

        $(
            let arrays = columns.next().unwrap().collect::<Vec<_>>();
            let arrays = arrays.into_iter().map(|a| a.unwrap()).collect::<Vec<_>>();
            let arrs = arrays.iter().map(|a| a.as_ref()).collect::<Vec<_>>();
            let array = concatenate(arrs.as_slice()).unwrap();

            let $name = if array.data_type() == &DataType::Binary {
                cast(array.as_ref(), &DataType::LargeBinary, Default::default()).unwrap().as_any().downcast_ref::<$arrow_type>().unwrap().clone()
            } else {
                array.as_any().downcast_ref::<$arrow_type>().unwrap().clone()
            };
        )*
    };
}

macro_rules! map_from_arrow {
    ($src_field:ident, $map_type:expr, $idx:expr) => {
        $map_type((&$src_field).get($idx).unwrap())
    };
}

macro_rules! map_from_arrow_opt {
    ($src_field:ident, $map_type:expr, $idx:expr) => {
        (&$src_field).get($idx).map($map_type)
    };
}

pub(crate) use define_cols;
pub(crate) use map_from_arrow;
pub(crate) use map_from_arrow_opt;

pub fn i64_to_bytes(num: i64) -> Bytes {
    let bytes = num.to_be_bytes();
    let idx = bytes
        .iter()
        .enumerate()
        .find(|(_, b)| **b != 0)
        .map(|b| b.0)
        .unwrap_or(bytes.len() - 1);
    let bytes = &bytes[idx..];
    Bytes::new(bytes)
}

pub fn i64_to_big_unsigned(num: i64) -> BigUnsigned {
    BigUnsigned(num.try_into().unwrap())
}
