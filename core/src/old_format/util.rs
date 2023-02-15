// defines columns using the result frame and a list of column names
macro_rules! define_cols {
    ($columns:expr, $($name:ident, $arrow_type:ident),*) => {
        $(
            let arrays = $columns.next().unwrap().collect::<Vec<_>>();
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
