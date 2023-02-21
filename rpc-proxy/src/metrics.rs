use crate::{Error, Result};
use core::sync::atomic::{AtomicI64};
use prometheus_client::encoding::text::encode;
use prometheus_client::encoding::{EncodeLabelSet, EncodeLabelValue};
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge as GaugeImpl;
use prometheus_client::registry::Registry;

type RPSGauge = GaugeImpl<i64, AtomicI64>;

pub struct Metrics {
    rps: Family<Label, RPSGauge>,
    registry: Registry,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
struct Label {
    kind: String,
}

impl Metrics {
    pub fn new() -> Self {
        let rps = Family::<Label, RPSGauge>::default();
        let mut registry = <Registry>::default();

        registry.register(
            "sqd_archive_requests_per_sec",
            "Requests per second",
            rps.clone(),
        );

        Self { rps, registry }
    }

    pub fn record_rps(&self, endpoint: &str, rps: usize) {
        self.rps.get_or_create(&Label {
                kind: endpoint.to_owned(),
        }).set(rps as i64);
    }

    pub fn encode(&self) -> Result<String> {
        let mut buf = String::new();

        encode(&mut buf, &self.registry).map_err(Error::EncodeMetrics)?;

        Ok(buf)
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}
