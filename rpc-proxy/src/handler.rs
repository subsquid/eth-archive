use crate::config::Config;
use crate::metrics::Metrics;
use crate::types::{MaybeBatch, RpcRequest, RpcResponse};
use crate::{Error, Result};
use actix_web::HttpRequest;
use eth_archive_core::retry::Retry;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub const TARGET_ENDPOINT_HEADER_NAME: &str = "eth_archive_rpc_proxy_target";

pub struct Handler {
    limiter: Arc<Mutex<Limiter>>,
    config: Config,
    http_client: Arc<reqwest::Client>,
    retry: Retry,
}

impl Handler {
    pub async fn new(config: Config, metrics: Arc<Metrics>) -> Result<Self> {
        let limiter = Limiter::new(config.max_requests_per_sec, metrics);
        let limiter = Arc::new(Mutex::new(limiter));

        let request_timeout = Duration::from_secs(config.request_timeout_secs.get());
        let connect_timeout = Duration::from_millis(config.connect_timeout_ms.get());

        let http_client = reqwest::ClientBuilder::new()
            .gzip(true)
            .timeout(request_timeout)
            .connect_timeout(connect_timeout)
            .build()
            .map_err(Error::BuildHttpClient)?;
        let http_client = Arc::new(http_client);

        let retry = Retry::new(config.retry);

        Ok(Self {
            limiter,
            config,
            http_client,
            retry,
        })
    }

    pub async fn handle(
        self: Arc<Self>,
        req: HttpRequest,
        rpc_req: MaybeBatch<RpcRequest>,
    ) -> Result<MaybeBatch<RpcResponse>> {
        let endpoint = match req.headers().get(TARGET_ENDPOINT_HEADER_NAME) {
            Some(endpoint) => endpoint
                .to_str()
                .map_err(|e| Error::InvalidHeaderValue(TARGET_ENDPOINT_HEADER_NAME, e))?
                .to_owned(),
            None => match &self.config.default_target_rpc {
                Some(endpoint) => endpoint.to_string(),
                None => return Err(Error::NoEndpointSpecified),
            },
        };

        let endpoint: Arc<str> = endpoint.into();

        match rpc_req {
            MaybeBatch::Batch(reqs) => {
                if self.config.separate_batches {
                    self.handle_separate_batches(endpoint, reqs)
                        .await
                        .map(MaybeBatch::Batch)
                } else {
                    self.handle_batches(endpoint, reqs)
                        .await
                        .map(MaybeBatch::Batch)
                }
            }
            MaybeBatch::Single(req) => self
                .handle_single(endpoint, req)
                .await
                .map(MaybeBatch::Single),
        }
    }

    async fn handle_separate_batches(
        self: Arc<Self>,
        _endpoint: Arc<str>,
        _reqs: Vec<RpcRequest>,
    ) -> Result<Vec<RpcResponse>> {
        todo!()
    }

    async fn handle_batches(
        self: Arc<Self>,
        endpoint: Arc<str>,
        reqs: Vec<RpcRequest>,
    ) -> Result<Vec<RpcResponse>> {
        let chunk_size = self.config.max_batch_size.unwrap_or(reqs.len());

        let mut resps = Vec::new();

        for chunk in reqs.chunks(chunk_size) {
            let req = Arc::new(MaybeBatch::Batch(chunk.to_vec()));
            match self.clone().send(endpoint.clone(), req.clone()).await? {
                MaybeBatch::Batch(resp_vec) => {
                    resps.extend_from_slice(&resp_vec);
                }
                MaybeBatch::Single(resps) => {
                    return Err(Error::InvalidRpcResponse(
                        serde_json::to_string(req.as_ref()).unwrap(),
                        serde_json::to_string(&resps).unwrap(),
                    ))
                }
            }
        }

        Ok(resps)
    }

    async fn handle_single(
        self: Arc<Self>,
        endpoint: Arc<str>,
        req: RpcRequest,
    ) -> Result<RpcResponse> {
        let req = Arc::new(MaybeBatch::Single(req));

        match self.send(endpoint, req.clone()).await? {
            MaybeBatch::Single(resp) => Ok(resp),
            MaybeBatch::Batch(resps) => Err(Error::InvalidRpcResponse(
                serde_json::to_string(req.as_ref()).unwrap(),
                serde_json::to_string(&resps).unwrap(),
            )),
        }
    }

    async fn send(
        self: Arc<Self>,
        endpoint: Arc<str>,
        req: Arc<MaybeBatch<RpcRequest>>,
    ) -> Result<MaybeBatch<RpcResponse>> {
        self.retry
            .retry(|| {
                let handler = self.clone();
                let req = req.clone();
                let endpoint = endpoint.clone();
                async move { handler.send_impl(&endpoint, &req).await }
            })
            .await
            .map_err(Error::Retry)
    }

    async fn send_impl(
        &self,
        endpoint: &str,
        req: &MaybeBatch<RpcRequest>,
    ) -> Result<MaybeBatch<RpcResponse>> {
        self.count_req(endpoint)?;

        let resp = self
            .http_client
            .post(endpoint)
            .json(req)
            .send()
            .await
            .map_err(Error::HttpRequest)?;

        let resp_status = resp.status();
        if !resp_status.is_success() {
            return Err(Error::RpcResponseStatus(
                resp_status.as_u16(),
                resp.text().await.ok(),
            ));
        }

        resp.json().await.map_err(Error::RpcResponseParse)
    }

    // this is a function to make sure the lock is released as soon as the request is counted
    fn count_req(&self, endpoint: &str) -> Result<()> {
        self.limiter.lock().unwrap().count_req(endpoint)
    }
}

struct Limiter {
    time: Instant,
    reqs_total: usize,
    reqs: HashMap<String, usize>,
    rps_limit: Option<usize>,
    metrics: Arc<Metrics>,
}

impl Limiter {
    fn new(rps_limit: Option<usize>, metrics: Arc<Metrics>) -> Self {
        Self {
            time: Instant::now(),
            reqs_total: 0,
            reqs: HashMap::new(),
            rps_limit,
            metrics,
        }
    }

    fn count_req(&mut self, endpoint: &str) -> Result<()> {
        if self.time.elapsed().as_millis() >= 1000 {
            for (endpoint, val) in self.reqs.iter() {
                self.metrics.record_rps(endpoint, *val);
            }
            self.metrics.record_rps("total", self.reqs_total);

            self.reqs_total = 0;
            self.reqs.values_mut().for_each(|val| *val = 0);
            self.time = Instant::now();
        }
        if let Some(rps_limit) = self.rps_limit {
            if self.reqs_total >= rps_limit {
                return Err(Error::RateLimited);
            }
        }

        self.reqs_total += 1;
        *self.reqs.entry(endpoint.to_owned()).or_default() += 1;

        Ok(())
    }
}
