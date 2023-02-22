use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum MaybeBatch<T> {
    Batch(Vec<T>),
    Single(T),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcRequest {
    id: usize,
    method: String,
    params: JsonValue,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcResponse {
    id: usize,
    result: JsonValue,
}
