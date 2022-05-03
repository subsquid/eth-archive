#[derive(Debug)]
pub enum Error {
    HttpRequest(reqwest::Error),
    RpcResponseStatus(u16),
    RpcResponseParse(reqwest::Error),
    RpcResultParse(serde_json::Error),
    RpcResponseInvalid,
    BuildHttpClient(reqwest::Error),
    GetTxBatch(Box<Error>),
    CreateParquetWriter(Box<parquet::errors::ParquetError>),
}
