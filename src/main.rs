use arrow2::io::parquet::write::*;
use eth_archive::eth_client::EthClient;
use eth_archive::eth_request::{GetBlockByNumber, GetLogs};
use eth_archive::schema::{Block, Blocks, IntoRowGroups, Log, Logs};
use serde_json::Value as JsonValue;
use std::fs;
use std::sync::Arc;

#[tokio::main]
async fn main() {
    let mut logs = Logs::default();

    let client =
        EthClient::new("https://eth-mainnet.alchemyapi.io/v2/DPijdCeN4cXDxSQT8eFfGETr2xhMegT0")
            .unwrap();

    let client = Arc::new(client);

    let resp = client
        .send(GetLogs {
            from_block: 1000,
            to_block: 1001,
        })
        .await
        .unwrap();

    for resp in resp {
        logs.push(resp).unwrap();
    }

    let (row_groups, schema, options) = logs.into_row_groups();

    let file = fs::File::create("data/test.parquet").unwrap();
    let mut writer = FileWriter::try_new(file, schema, options).unwrap();

    writer.start().unwrap();
    for group in row_groups {
        writer.write(group.unwrap()).unwrap();
    }
    writer.end(None).unwrap();
}
