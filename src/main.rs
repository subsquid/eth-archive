use arrow2::io::parquet::write::*;
use eth_archive::schema::{Blocks, IntoRowGroups};
use std::fs;

fn main() {
    let mut blocks = Blocks::default();

    let (row_groups, schema, options) = blocks.into_row_groups();

    let file = fs::File::create("data/test.parquet").unwrap();
    let mut writer = FileWriter::try_new(file, schema, options).unwrap();

    writer.start().unwrap();
    for group in row_groups {
        writer.write(group.unwrap()).unwrap();
    }
    writer.end(None).unwrap();
}
