use crate::error::{Error, Result};
use crate::eth_request::{EthRequest, GetBestBlock};
use serde_json::Value as JsonValue;

pub struct EthClient {
    http_client: reqwest::Client,
    rpc_url: url::Url,
}

impl EthClient {
    pub fn new(rpc_url: url::Url) -> Result<EthClient> {
        let http_client = reqwest::ClientBuilder::new()
            .gzip(true)
            .build()
            .map_err(Error::BuildHttpClient)?;

        Ok(EthClient {
            http_client,
            rpc_url,
        })
    }

    pub async fn send<R: EthRequest>(&self, req: R) -> Result<R::Resp> {
        let resp = self
            .http_client
            .post(self.rpc_url.clone())
            .json(&req.to_body(1))
            .send()
            .await
            .map_err(Error::HttpRequest)?;

        let resp_status = resp.status();
        if !resp_status.is_success() {
            let body = resp.text().await.ok();
            return Err(Error::RpcResponseStatus(resp_status.as_u16(), body));
        }

        let rpc_result = resp.json().await.map_err(Error::RpcResponseParse)?;

        let mut rpc_result = match rpc_result {
            JsonValue::Object(rpc_result) => rpc_result,
            _ => return Err(Error::InvalidRpcResponse),
        };

        let rpc_result = rpc_result
            .remove("result")
            .ok_or(Error::InvalidRpcResponse)?;

        let rpc_result = serde_json::from_value(rpc_result).map_err(Error::RpcResultParse)?;

        Ok(rpc_result)
    }

    pub async fn send_batch<R: EthRequest>(&self, requests: &[R]) -> Result<Vec<R::Resp>> {
        let req_body = requests
            .iter()
            .enumerate()
            .map(|(i, req)| req.to_body(i + 1))
            .collect::<Vec<_>>();
        let req_body = JsonValue::Array(req_body);

        let resp = self
            .http_client
            .post(self.rpc_url.clone())
            .json(&req_body)
            .send()
            .await
            .map_err(Error::HttpRequest)?;

        let resp_status = resp.status();
        if !resp_status.is_success() {
            let body = resp.text().await.ok();
            return Err(Error::RpcResponseStatus(resp_status.as_u16(), body));
        }

        let resp_body = resp
            .json::<Vec<JsonValue>>()
            .await
            .map_err(Error::RpcResponseParse)?;

        let rpc_results = resp_body
            .into_iter()
            .map(|rpc_result| {
                let mut rpc_result = match rpc_result {
                    JsonValue::Object(rpc_result) => rpc_result,
                    _ => return Err(Error::InvalidRpcResponse),
                };

                let rpc_result = rpc_result
                    .remove("result")
                    .ok_or(Error::InvalidRpcResponse)?;

                serde_json::from_value(rpc_result).map_err(Error::RpcResponseParseJson)
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(rpc_results)
    }

    pub async fn get_best_block(&self) -> Result<usize> {
        let num = self.send(GetBestBlock {}).await?;
        Ok(get_usize_from_hex(&num))
    }
}

fn get_usize_from_hex(hex: &str) -> usize {
    let without_prefix = hex.trim_start_matches("0x");
    usize::from_str_radix(without_prefix, 16).unwrap()
}
