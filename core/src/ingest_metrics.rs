use crate::{Error, Result};
use core::sync::atomic::{AtomicU32, AtomicU64};
use prometheus_client::encoding::text::{encode, Encode};
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge as GaugeImpl;
use prometheus_client::registry::Registry;

type HeightGauge = GaugeImpl<u32, AtomicU32>;
type IngestGauge = GaugeImpl<f64, AtomicU64>;

pub struct IngestMetrics {
    ingest: Family<IngestLabel, IngestGauge>,
    height: Family<HeightLabel, HeightGauge>,
    registry: Registry,
}

#[derive(Clone, Hash, PartialEq, Eq, Encode)]
enum IngestLabel {
    Download,
    Write,
}

#[derive(Clone, Hash, PartialEq, Eq, Encode)]
enum HeightLabel {
    Download,
    Write,
    Chain,
}

impl IngestMetrics {
    pub fn new() -> Self {
        let ingest = Family::<IngestLabel, IngestGauge>::default();
        let height = Family::<HeightLabel, HeightGauge>::default();
        let mut registry = <Registry>::default();

        registry.register(
            "ingest_speed",
            "Blocks processed per second",
            Box::new(ingest.clone()),
        );

        registry.register(
            "block_height",
            "Number of the latest processed block",
            Box::new(height.clone()),
        );

        Self {
            ingest,
            height,
            registry,
        }
    }

    pub fn record_download_speed(&self, blocks_per_second: f64) {
        self.ingest
            .get_or_create(&IngestLabel::Download)
            .set(blocks_per_second);
    }

    pub fn record_write_speed(&self, blocks_per_second: f64) {
        self.ingest
            .get_or_create(&IngestLabel::Write)
            .set(blocks_per_second);
    }

    pub fn record_download_height(&self, height: u32) {
        self.height
            .get_or_create(&HeightLabel::Download)
            .set(height);
    }

    pub fn record_write_height(&self, height: u32) {
        self.height.get_or_create(&HeightLabel::Write).set(height);
    }

    pub fn record_chain_height(&self, height: u32) {
        self.height.get_or_create(&HeightLabel::Chain).set(height);
    }

    pub fn encode(&self) -> Result<String> {
        let mut buf = Vec::new();

        encode(&mut buf, &self.registry).map_err(Error::EncodeMetrics)?;
        let s = String::from_utf8(buf).map_err(Error::MetricsUtf8)?;

        Ok(s)
    }
}

impl Default for IngestMetrics {
    fn default() -> Self {
        Self::new()
    }
}
