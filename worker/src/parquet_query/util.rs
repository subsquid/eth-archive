// defines columns using the result frame and a list of column names
macro_rules! define_cols {
    ($columns:expr, $($name:ident, $arrow_type:ident),*) => {
        $(
            let $name = $columns.remove(stringify!($name)).map(|arrays| {
                let arrays = arrays.into_iter().map(|a| a.unwrap()).collect::<Vec<_>>();
                let arrs = arrays.iter().map(|a| a.as_ref()).collect::<Vec<_>>();
                let array = concatenate(arrs.as_slice()).unwrap();

                array.as_any().downcast_ref::<$arrow_type>().unwrap().clone()
            });
        )*
    };
}

macro_rules! map_from_arrow {
    ($src_field:ident, $map_type:expr, $idx:expr) => {
        $src_field
            .as_ref()
            .map(|arr| $map_type(arr.get($idx).unwrap()))
    };
}

macro_rules! map_from_arrow_opt {
    ($src_field:ident, $map_type:expr, $idx:expr) => {
        $src_field
            .as_ref()
            .map(|arr| arr.get($idx).map($map_type))
            .flatten()
    };
}

pub(crate) use define_cols;
pub(crate) use map_from_arrow;
pub(crate) use map_from_arrow_opt;
