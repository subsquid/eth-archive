use crate::config::Config;
use crate::metrics::Metrics;
use crate::types::{RpcRequest, RpcResponse, MaybeBatch};
use crate::{Error, Result};
use actix_web::HttpRequest;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub const TARGET_ENDPOINT_HEADER_NAME: &str = "eth_archive_rpc_proxy_target";

pub struct Handler {
    limiter: Arc<Mutex<Limiter>>,
    config: Config,
    http_client: reqwest::Client,
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

        Ok(Self {
            limiter,
            config,
            http_client,
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
        endpoint: String,
        reqs: Vec<RpcRequest>,
    ) -> Result<Vec<RpcResponse>> {
        todo!()
    }

    async fn handle_batches(
        self: Arc<Self>,
        endpoint: String,
        reqs: Vec<RpcRequest>,
    ) -> Result<Vec<RpcResponse>> {
        todo!()
    }

    async fn handle_single(
        self: Arc<Self>,
        endpoint: String,
        req: RpcRequest,
    ) -> Result<RpcResponse> {
        self.count_req(&endpoint)?;

        todo!()
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

enum CountResult {
    Counted,
    Limited,
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
