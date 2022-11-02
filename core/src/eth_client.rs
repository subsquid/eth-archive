use crate::config::IngestConfig;
use crate::error::{Error, Result};
use crate::eth_request::{EthRequest, GetBestBlock, GetBlockByNumber, GetLogs};
use crate::retry::Retry;
use crate::types::{Block, BlockRange, Log};
use futures::stream::Stream;
use serde_json::Value as JsonValue;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use std::{cmp, env};

const ETH_RPC_URL: &str = "ETH_RPC_URL";

pub struct EthClient {
    http_client: reqwest::Client,
    rpc_url: url::Url,
    cfg: IngestConfig,
    retry: Retry,
}

impl EthClient {
    pub fn new(cfg: IngestConfig, retry: Retry) -> Result<EthClient> {
        let request_timeout = Duration::from_secs(cfg.request_timeout_secs.get());
        let connect_timeout = Duration::from_millis(cfg.connect_timeout_ms.get());

        let rpc_url = env::var(ETH_RPC_URL).map_err(Error::ReadRpcUrlFromEnv)?;
        let rpc_url = url::Url::parse(&rpc_url).map_err(Error::ParseRpcUrl)?;

        let http_client = reqwest::ClientBuilder::new()
            .gzip(true)
            .timeout(request_timeout)
            .connect_timeout(connect_timeout)
            .build()
            .map_err(Error::BuildHttpClient)?;

        Ok(EthClient {
            http_client,
            rpc_url,
            cfg,
            retry,
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
            .json::<JsonValue>()
            .await
            .map_err(Error::RpcResponseParse)?;

        let resp_body = match resp_body {
            JsonValue::Array(resp) => resp,
            _ => {
                let body = serde_json::to_string_pretty(&resp_body).unwrap();
                log::error!("invalid rpc response, body was:\n{}", body);
                return Err(Error::InvalidRpcResponse);
            }
        };

        let rpc_results = resp_body
            .into_iter()
            .map(|rpc_response| {
                let mut rpc_response = match rpc_response {
                    JsonValue::Object(rpc_response) => rpc_response,
                    _ => return Err(Error::InvalidRpcResponse),
                };

                let rpc_result = rpc_response
                    .remove("result")
                    .ok_or(Error::InvalidRpcResponse)?;

                match serde_json::from_value(rpc_result) {
                    Ok(res) => Ok(res),
                    Err(e) => {
                        log::error!(
                            "failed to parse rpc response, body was:\n{}",
                            serde_json::to_string_pretty(&rpc_response).unwrap()
                        );
                        Err(Error::RpcResponseParseJson(e))
                    }
                }
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(rpc_results)
    }

    pub async fn send_batches<R: EthRequest, B: AsRef<[R]>>(
        self: Arc<Self>,
        batches: &[B],
    ) -> Result<Vec<Vec<R::Resp>>> {
        let group = batches.iter().map(|batch| {
            let client = self.clone();
            self.retry.retry(move || {
                let client = client.clone();
                async move { client.send_batch(batch.as_ref()).await }
            })
        });
        let group = futures::future::join_all(group).await;
        group.into_iter().map(|g| g.map_err(Error::Retry)).collect()
    }

    pub async fn send_concurrent<R: EthRequest + Copy>(
        self: Arc<Self>,
        reqs: &[R],
    ) -> Result<Vec<R::Resp>> {
        let group = reqs.iter().map(|&req| {
            let client = self.clone();
            self.retry.retry(move || {
                let client = client.clone();
                async move { client.send(req).await }
            })
        });
        let group = futures::future::join_all(group).await;
        group.into_iter().map(|g| g.map_err(Error::Retry)).collect()
    }

    pub async fn get_best_block(self: Arc<Self>) -> Result<u32> {
        let client = self.clone();
        let offset = self.cfg.best_block_offset;
        self.retry
            .retry(move || {
                let client = client.clone();
                async move {
                    let num = client.send(GetBestBlock {}).await?;
                    let num = get_u32_from_hex(&num);

                    let num = if num > offset { num - offset } else { 0 };

                    Ok(num)
                }
            })
            .await
            .map_err(Error::GetBestBlock)
    }

    pub fn stream_batches(
        self: Arc<Self>,
        from_block: Option<u32>,
        to_block: Option<u32>,
    ) -> impl Stream<Item = Result<(Vec<BlockRange>, Vec<Vec<Block>>, Vec<Vec<Log>>)>> {
        let from_block = from_block.unwrap_or(0);

        let step = self.cfg.http_req_concurrency * self.cfg.block_batch_size;
        async_stream::try_stream! {
            let mut block_num = from_block;
            loop {
                match to_block {
                    Some(to_block) if block_num >= to_block => break,
                    _ => (),
                }

                let best_block = self.clone().get_best_block().await?;
                let to_block = match to_block {
                    Some(to_block) => cmp::min(to_block, best_block),
                    None => best_block,
                };

                let concurrency = self.cfg.http_req_concurrency;
                let batch_size = self.cfg.block_batch_size;

                while block_num > best_block {
                    let offset = self.cfg.best_block_offset;
                    log::info!("waiting for chain tip to reach {}, current value is {}", block_num + offset, best_block + offset);
                    tokio::time::sleep(Duration::from_secs(3)).await;
                }

                if block_num + batch_size > best_block {
                    let block = self.send(GetBlockByNumber { block_number: block_num }).await?;
                    let logs = self.send(GetLogs {
                        from_block: block_num,
                        to_block: block_num,
                    }).await?;

                    block_num += 1;

                    yield (
                        vec![BlockRange{from: block_num, to: block_num}],
                        vec![vec![block]],
                        vec![logs],
                    );
                }

                let block_batches = (0..concurrency)
                    .filter_map(|step_no: u32| {
                        let start = block_num + step_no * batch_size;
                        let end = cmp::min(start + batch_size, to_block);

                        let batch = (start..end)
                            .map(|i| GetBlockByNumber { block_number: i })
                            .collect::<Vec<_>>();

                        if batch.is_empty() {
                            None
                        } else {
                            Some(batch)
                        }
                    })
                    .collect::<Vec<_>>();

                let log_batches = (0..concurrency)
                    .filter_map(|step_no| {
                        let start = block_num + step_no * batch_size;
                        let end = cmp::min(start + batch_size, to_block);

                        if start < end {
                            Some(GetLogs {
                                from_block: start,
                                to_block: end - 1,
                            })
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>();

                let start_time = Instant::now();

                let ended_block = cmp::min(block_num + step, to_block);

                log::info!(
                    "starting to download blocks {}-{}",
                    block_num,
                    ended_block,
                );

                let block_batches = self
                    .clone()
                    .send_batches(&block_batches)
                    .await?;
                let log_batches = self
                    .clone()
                    .send_concurrent(&log_batches)
                    .await?;

                log::info!(
                    "downloaded blocks {}-{} in {}ms",
                    block_num,
                    ended_block,
                    start_time.elapsed().as_millis()
                );

                let num_batches = u32::try_from(block_batches.len()).unwrap();

                let block_ranges = (0..num_batches).map(|i| {
                    let start = block_num + i * batch_size;
                    let end = cmp::min(start + batch_size, to_block);
                    let block_range = BlockRange {
                        from: start,
                        to: end,
                    };
                    block_range
                }).collect();

                block_num = ended_block;

                yield (
                    block_ranges,
                    block_batches,
                    log_batches,
                );
            }
        }
    }
}

fn get_u32_from_hex(hex: &str) -> u32 {
    let without_prefix = hex.trim_start_matches("0x");
    u32::from_str_radix(without_prefix, 16).unwrap()
}
