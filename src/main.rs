use arrow2::array::*;
use arrow2::io::parquet::write::*;
use eth_archive::schema::Blocks;
use std::fs;

fn main() {
    let mut nonce = MutableUtf8Array::new();
    nonce.try_push(Some("asd".to_owned())).unwrap();
    nonce.try_push(Some("qwe".to_owned())).unwrap();

    let mut size = MutableUtf8Array::new();
    size.try_push(Some("zxc".to_owned())).unwrap();
    size.try_push(Some("möç".to_owned())).unwrap();

    let (row_groups, schema, options) = Blocks {
        number: vec![3, 4],
        timestamp: vec![5, 6],
        nonce,
        size,
    }
    .into_row_groups();

    let file = fs::File::create("data/test.parquet").unwrap();
    let mut writer = FileWriter::try_new(file, schema, options).unwrap();

    writer.start().unwrap();
    for group in row_groups {
        writer.write(group.unwrap()).unwrap();
    }
    writer.end(None).unwrap();
}
