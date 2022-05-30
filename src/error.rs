use std::result::Result as StdResult;

#[derive(Debug)]
pub enum Error {
    PushRow(arrow2::error::ArrowError),
    HttpRequest(reqwest::Error),
    RpcResponseStatus(u16),
    RpcResponseParse(reqwest::Error),
    RpcResultParse(serde_json::Error),
    RpcResponseInvalid,
    BuildHttpClient(reqwest::Error),
    GetTxBatch(Box<Error>),
    InvalidRpcResponse,
    SortRowGroup(arrow2::error::ArrowError),
}

pub type Result<T> = StdResult<T, Error>;
