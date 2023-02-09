use crate::config::IngestConfig;
use crate::error::{Error, Result};
use crate::eth_request::{EthRequest, GetBestBlock, GetBlockByNumber, GetBlockReceipts, GetLogs};
use crate::ingest_metrics::IngestMetrics;
use crate::retry::Retry;
use crate::types::{Block, BlockRange, Log};
use futures::stream::Stream;
use rand::seq::SliceRandom;
use serde_json::Value as JsonValue;
use std::cmp;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use url::Url;

pub struct EthClient {
    http_client: reqwest::Client,
    cfg: IngestConfig,
    retry: Retry,
    metrics: Arc<IngestMetrics>,
}

struct UrlSet {
    inner: Vec<Url>,
    best_block: u32,
}

impl UrlSet {
    fn get_random(&self) -> Url {
        self.inner.choose(&mut rand::thread_rng()).unwrap().clone()
    }
}

impl EthClient {
    pub fn new(cfg: IngestConfig, retry: Retry, metrics: Arc<IngestMetrics>) -> Result<EthClient> {
        let request_timeout = Duration::from_secs(cfg.request_timeout_secs.get());
        let connect_timeout = Duration::from_millis(cfg.connect_timeout_ms.get());

        let http_client = reqwest::ClientBuilder::new()
            .gzip(true)
            .timeout(request_timeout)
            .connect_timeout(connect_timeout)
            .build()
            .map_err(Error::BuildHttpClient)?;

        Ok(EthClient {
            http_client,
            cfg,
            retry,
            metrics,
        })
    }

    async fn healthy_url_set_impl(&self) -> Result<Arc<UrlSet>> {
        let mut best_blocks = Vec::new();
        for url in self.cfg.rpc_urls.iter() {
            match self.get_best_block_url(url.clone()).await {
                Ok(best_block) => best_blocks.push((best_block, url.clone())),
                Err(e) => {
                    log::warn!(
                        "excluding rpc url {} because couldn't get best block:\n{}",
                        url,
                        e
                    );
                }
            }
        }

        let max = best_blocks
            .iter()
            .max_by_key(|bb| bb.0)
            .ok_or(Error::NoHealthyUrl)?
            .0;

        let best_blocks = best_blocks
            .into_iter()
            .filter(|bb| {
                if max - bb.0 > 5 {
                    log::warn!("excluding rpc url {} because it is too behind.", bb.1);
                    return false;
                }

                true
            })
            .collect::<Vec<_>>();

        let best_block = best_blocks
            .iter()
            .min_by_key(|bb| bb.0)
            .ok_or(Error::NoHealthyUrl)?
            .0;
        let inner = best_blocks.into_iter().map(|bb| bb.1).collect();

        Ok(Arc::new(UrlSet { best_block, inner }))
    }

    async fn healthy_url_set(self: Arc<Self>) -> Result<Arc<UrlSet>> {
        self.retry
            .retry(|| {
                let client = self.clone();
                async move { client.healthy_url_set_impl().await }
            })
            .await
            .map_err(Error::Retry)
    }

    async fn send_impl<R: EthRequest>(&self, url: Url, req: R) -> Result<R::Resp> {
        let resp = self
            .http_client
            .post(url)
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

    async fn send<R: EthRequest + Copy>(
        self: Arc<Self>,
        url_set: Arc<UrlSet>,
        req: R,
    ) -> Result<R::Resp> {
        self.retry
            .retry(|| {
                let client = self.clone();
                let url = url_set.get_random();
                async move { client.send_impl(url, req).await }
            })
            .await
            .map_err(Error::Retry)
    }

    async fn send_batch<R: EthRequest>(&self, url: Url, requests: &[R]) -> Result<Vec<R::Resp>> {
        let req_body = requests
            .iter()
            .enumerate()
            .map(|(i, req)| req.to_body(i + 1))
            .collect::<Vec<_>>();
        let req_body = JsonValue::Array(req_body);

        let resp = self
            .http_client
            .post(url)
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

    async fn send_batches<R: EthRequest, B: AsRef<[R]>>(
        self: Arc<Self>,
        url_set: Arc<UrlSet>,
        batches: &[B],
    ) -> Result<Vec<Vec<R::Resp>>> {
        let group = batches.iter().map(|batch| {
            let client = self.clone();
            let url_set = url_set.clone();
            self.retry.retry(move || {
                let client = client.clone();
                let url = url_set.get_random();
                async move { client.send_batch(url, batch.as_ref()).await }
            })
        });
        let group = futures::future::join_all(group).await;
        group.into_iter().map(|g| g.map_err(Error::Retry)).collect()
    }

    async fn send_concurrent<R: EthRequest + Copy>(
        self: Arc<Self>,
        url_set: Arc<UrlSet>,
        reqs: &[R],
    ) -> Result<Vec<R::Resp>> {
        let group = reqs.iter().map(|&req| {
            let client = self.clone();
            client.send(url_set.clone(), req)
        });
        let group = futures::future::join_all(group).await;
        group.into_iter().collect()
    }

    pub async fn get_best_block(self: Arc<Self>) -> Result<u32> {
        let url_set = self.healthy_url_set().await?;

        Ok(url_set.best_block)
    }

    async fn get_best_block_url(&self, url: Url) -> Result<u32> {
        let offset = self.cfg.best_block_offset;

        let num = self.send_impl(url, GetBestBlock {}).await?;
        let num = get_u32_from_hex(&num);

        self.metrics.record_chain_height(num);

        let num = if num > offset { num - offset } else { 0 };

        Ok(num)
    }

    pub fn stream_batches(
        self: Arc<Self>,
        from: Option<u32>,
        to: Option<u32>,
    ) -> impl Stream<Item = Result<(Vec<BlockRange>, Vec<Vec<Block>>, Vec<Vec<Log>>)>> {
        let from_block = from.unwrap_or(0);

        let step = self.cfg.http_req_concurrency * self.cfg.block_batch_size;
        async_stream::try_stream! {
            let mut block_num = from_block;
            loop {
                match to {
                    Some(to_block) if block_num >= to_block => break,
                    _ => (),
                }

                let concurrency = self.cfg.http_req_concurrency;
                let batch_size = self.cfg.block_batch_size;

                let (url_set, to_block) = loop {
                    let url_set = self.clone().healthy_url_set().await?;

                    let best_block = url_set.best_block;
                    let to_block = match to {
                        Some(to_block) => cmp::min(to_block, best_block+1),
                        None => best_block+1,
                    };

                    if block_num < to_block {
                        break (url_set, to_block);
                    } else {
                        tokio::time::sleep(Duration::from_secs(10)).await;
                    }
                };

                if block_num + batch_size >= to_block {
                    let start_time = Instant::now();

                    let mut block = self.clone().send(url_set.clone(), GetBlockByNumber { block_number: block_num }).await?;

                    let logs = if self.cfg.get_receipts {
                        let receipts = self.clone().send(url_set, GetBlockReceipts {
                            block_number: block_num
                        }).await?;

                        block.transactions.iter_mut().zip(receipts.into_iter()).filter_map(|(tx, receipt)| {
                            assert_eq!(tx.block_number, receipt.block_number);
                            assert_eq!(tx.transaction_index, receipt.transaction_index);

                            tx.status = receipt.status;

                            receipt.logs.map(|logs| logs.into_iter())
                        }).flatten().collect()
                    } else {
                        self.clone().send(url_set, GetLogs {
                            from_block: block_num,
                            to_block: block_num,
                        }).await?
                    };

                    self.metrics.record_download_height(block_num);

                    let elapsed = start_time.elapsed().as_millis();
                    if elapsed > 0 {
                        self.metrics.record_download_speed(1. / elapsed as f64 * 1000.);
                    }

                    let block_range = BlockRange {
                        from: block_num,
                        to: block_num+1,
                    };

                    block_num += 1;

                    yield (
                        vec![block_range],
                        vec![vec![block]],
                        vec![logs],
                    );

                    continue;
                }

                let start_time = Instant::now();

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

                let receipt_batches = (0..concurrency)
                    .filter_map(|step_no: u32| {
                        let start = block_num + step_no * batch_size;
                        let end = cmp::min(start + batch_size, to_block);

                        let batch = (start..end)
                            .map(|i| GetBlockReceipts { block_number: i })
                            .collect::<Vec<_>>();

                        if batch.is_empty() {
                            None
                        } else {
                            Some(batch)
                        }
                    })
                    .collect::<Vec<_>>();

                let mut block_batches = self
                    .clone()
                    .send_batches(url_set.clone(), &block_batches)
                    .await?;

                let log_batches = if self.cfg.get_receipts {
                    let receipt_batches = self
                        .clone()
                        .send_batches(url_set.clone(), &receipt_batches)
                        .await?;

                    block_batches.iter_mut().zip(receipt_batches.into_iter()).map(|(block_batch, receipt_batch)| {
                        block_batch.iter_mut().zip(receipt_batch.into_iter()).flat_map(|(block, receipts)| {
                            block.transactions.iter_mut().zip(receipts.into_iter()).filter_map(|(tx, receipt)| {
                                assert_eq!(tx.block_number, receipt.block_number);
                                assert_eq!(tx.transaction_index, receipt.transaction_index);

                                tx.status = receipt.status;

                                receipt.logs.map(|logs| logs.into_iter())
                            }).flatten()
                        }).collect()
                    }).collect()
                } else {
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

                    self
                        .clone()
                        .send_concurrent(url_set, &log_batches)
                        .await?
                };

                let ended_block = cmp::min(block_num + step, to_block);

                if ended_block > 0 {
                    self.metrics.record_download_height(ended_block-1);
                }
                let elapsed = start_time.elapsed().as_millis();
                if elapsed > 0 {
                    self.metrics.record_download_speed((ended_block-block_num) as f64 / elapsed as f64 * 1000.);
                }

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

                if let Some(secs) = self.cfg.wait_between_rounds {
                    tokio::time::sleep(Duration::from_secs(secs)).await;
                }
            }
        }
    }
}

fn get_u32_from_hex(hex: &str) -> u32 {
    let without_prefix = hex.trim_start_matches("0x");
    u32::from_str_radix(without_prefix, 16).unwrap()
}
