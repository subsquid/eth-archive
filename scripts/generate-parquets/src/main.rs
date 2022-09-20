use data::read_data_from_file;
use parquet::generate_parquets;

mod data;
mod parquet;

#[tokio::main]
async fn main() {
    let data = read_data_from_file("./scripts/generate-parquets/data.json");
    generate_parquets(data, "./gateway/tests/data").await;
}
