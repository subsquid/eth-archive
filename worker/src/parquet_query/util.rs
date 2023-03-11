// defines columns using the result frame and a list of column names
macro_rules! define_cols {
    ($columns:expr, $($name:ident, $arrow_type:ident),*) => {
        $(
            let $name = $columns.get(stringify!($name)).map(|arr| {
                arr.as_any()
                    .downcast_ref::<$arrow_type>()
                    .unwrap()
            });
        )*
    };
}

macro_rules! map_from_arrow {
    ($src_field:ident, $map_type:expr, $idx:expr) => {
        $src_field.map(|arr| arr.get($idx).unwrap()).map($map_type)
    };
}

macro_rules! map_from_arrow_opt {
    ($src_field:ident, $map_type:expr, $idx:expr) => {
        match $src_field.map(|arr| arr.get($idx)) {
            Some(Some(val)) => Some($map_type(val)),
            _ => None,
        }
    };
}

pub(crate) use define_cols;
pub(crate) use map_from_arrow;
pub(crate) use map_from_arrow_opt;
