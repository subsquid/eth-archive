use crate::error::Error;
use crate::eth_request::EthRequest;
use serde_json::Value as JsonValue;

pub struct EthClient {
    http_client: reqwest::Client,
    rpc_url: String,
}

impl EthClient {
    pub fn new<S: Into<String>>(rpc_url: S) -> Result<EthClient, Error> {
        let http_client = reqwest::ClientBuilder::new()
            .gzip(true)
            .build()
            .map_err(Error::BuildHttpClient)?;

        Ok(EthClient {
            http_client,
            rpc_url: rpc_url.into(),
        })
    }

    pub async fn send<R: EthRequest>(&self, req: R) -> Result<R::Resp, Error> {
        let resp = self
            .http_client
            .post(&self.rpc_url)
            .json(&req.to_body(1))
            .send()
            .await
            .map_err(Error::HttpRequest)?;

        let resp_status = resp.status();
        if !resp_status.is_success() {
            return Err(Error::RpcResponseStatus(resp_status.as_u16()));
        }

        let rpc_result = resp.json().await.map_err(Error::RpcResponseParse)?;

        let mut rpc_result = match rpc_result {
            JsonValue::Object(rpc_result) => rpc_result,
            _ => return Err(Error::InvalidRpcResponse),
        };

        let rpc_result = rpc_result
            .remove("result")
            .ok_or(Error::InvalidRpcResponse)?;

        let rpc_result =
            serde_json::from_value(rpc_result).map_err(|_| Error::InvalidRpcResponse)?;

        Ok(rpc_result)
    }

    pub async fn send_batch<R: EthRequest>(&self, requests: Vec<R>) -> Result<Vec<R::Resp>, Error> {
        let req_body = requests
            .iter()
            .enumerate()
            .map(|(i, req)| req.to_body(i + 1))
            .collect::<Vec<_>>();
        let req_body = JsonValue::Array(req_body);

        let resp = self
            .http_client
            .post(&self.rpc_url)
            .json(&req_body)
            .send()
            .await
            .map_err(Error::HttpRequest)?;

        let resp_status = resp.status();
        if !resp_status.is_success() {
            return Err(Error::RpcResponseStatus(resp_status.as_u16()));
        }

        let resp_body = resp
            .json::<Vec<JsonValue>>()
            .await
            .map_err(Error::RpcResponseParse)?;

        let rpc_results = resp_body
            .into_iter()
            .filter_map(|rpc_result| {
                let mut rpc_result = match rpc_result {
                    JsonValue::Object(rpc_result) => rpc_result,
                    _ => return None,
                };

                let rpc_result = rpc_result.remove("result")?;

                match serde_json::from_value(rpc_result) {
                    Ok(v) => Some(v),
                    Err(e) => {
                        eprintln!("failed to parse block in rpc response: {:#?}", e,);
                        None
                    }
                }
            })
            .collect();

        Ok(rpc_results)
    }
}
