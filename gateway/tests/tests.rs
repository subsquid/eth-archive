use common::{launch_gateway, Client};
use serde_json::json;

mod common;

#[actix_web::test]
async fn log_address_filtering() {
    launch_gateway();
    let client = Client::new();
    let address = "0xdac17f958d2ee523a2206206994597c13d831ec7".to_string();
    let response = client
        .query(json!({
            "fromBlock": 0,
            "logs": [{
                "address": address,
                "topics": [],
                "fieldSelection": {
                    "log": {
                        "address": true,
                    }
                }
            }]
        }))
        .await;
    let logs = &response.data[0].logs;
    assert!(logs.len() == 1);
    assert!(logs[0].address == Some(address));
}

#[actix_web::test]
async fn log_topics_filtering() {
    launch_gateway();
    let client = Client::new();
    let response = client
        .query(json!({
            "fromBlock": 0,
            "logs": [{
                "address": "0xdac17f958d2ee523a2206206994597c13d831ec7",
                "topics": [["0x0000000000000000000000000000000000000000000000000000000000000000"]],
                "fieldSelection": {
                    "log": {
                        "address": true,
                    }
                }
            }]
        }))
        .await;
    assert!(response.data.len() == 0);
}
