use crate::config::Config;
use crate::metrics::Metrics;
use crate::types::{RpcRequest, RpcResponse};
use crate::{Error, Result};
use actix_web::HttpRequest;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

pub const TARGET_ENDPOINT_HEADER_NAME: &str = "eth_archive_rpc_proxy_target";

pub struct Handler {
    limiter: Arc<Mutex<Limiter>>,
    config: Config,
}

impl Handler {
    pub async fn new(config: Config, metrics: Arc<Metrics>) -> Result<Self> {
        let limiter = Limiter::new(config.max_requests_per_sec, metrics);
        let limiter = Arc::new(Mutex::new(limiter));

        Ok(Self { limiter, config })
    }

    pub async fn handle(
        self: Arc<Self>,
        req: HttpRequest,
        rpc_req: RpcRequest,
    ) -> Result<RpcResponse> {
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

        // TODO: impl separate_batches config

        {
            if let CountResult::Limited = self.limiter.lock().unwrap().count_req(&endpoint) {
                return Err(Error::RateLimited);
            }
        }

        todo!()
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

    fn count_req(&mut self, endpoint: &str) -> CountResult {
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
                return CountResult::Limited;
            }
        }

        self.reqs_total += 1;
        *self.reqs.entry(endpoint.to_owned()).or_default() += 1;

        CountResult::Counted
    }
}
