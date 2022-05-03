use crate::error::Error;
use crate::eth_request::{EthRequest, GetBlockByNumber, Transaction};
use serde_json::Value as JsonValue;
use std::cmp;
use std::future::Future;
use std::iter::Iterator;
use std::pin::Pin;
use std::sync::Arc;

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

    pub async fn send<R: EthRequest>(&self, req: R) -> Result<JsonValue, Error> {
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

        let resp_body = resp.json().await.map_err(Error::RpcResponseParse)?;

        Ok(resp_body)
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

    pub fn get_txs_in_range(
        self: Arc<Self>,
        start: usize,
        end: usize,
        batch_size: usize,
    ) -> TxsInRange {
        TxsInRange {
            client: self,
            block_num: start,
            end,
            batch_size,
        }
    }
}

pub struct TxsInRange {
    client: Arc<EthClient>,
    block_num: usize,
    end: usize,
    batch_size: usize,
}

type TxsFuture = Pin<Box<dyn Future<Output = Result<Vec<Transaction>, Error>> + Send + Sync>>;

impl Iterator for TxsInRange {
    type Item = TxsFuture;

    fn next(&mut self) -> Option<Self::Item> {
        if self.block_num >= self.end {
            return None;
        }

        let end = cmp::min(self.end, self.block_num + self.batch_size);

        let client = self.client.clone();
        let block_num = self.block_num;

        let fut = async move {
            let blocks = client
                .send_batch(
                    (block_num..end)
                        .map(|block_number| GetBlockByNumber { block_number })
                        .collect(),
                )
                .await?;

            let mut txs = Vec::new();

            for block in blocks {
                txs.extend_from_slice(&block.transactions);
            }

            Ok(txs)
        };

        self.block_num += self.batch_size;

        Some(Box::pin(fut))
    }
}
