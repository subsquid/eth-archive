use crate::{Error, Result};
use core::sync::atomic::{AtomicI64, AtomicU64};
use prometheus_client::encoding::text::encode;
use prometheus_client::encoding::{EncodeLabelSet, EncodeLabelValue};
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge as GaugeImpl;
use prometheus_client::registry::Registry;

type HeightGauge = GaugeImpl<i64, AtomicI64>;
type IngestGauge = GaugeImpl<f64, AtomicU64>;

pub struct Metrics {
    ingest: Family<Label, IngestGauge>,
    height: Family<Label, HeightGauge>,
    registry: Registry,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelValue)]
enum LabelKind {
    Download,
    Write,
    Chain,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
struct Label {
    kind: LabelKind,
}

impl IngestMetrics {
    pub fn new() -> Self {
        let ingest = Family::<Label, IngestGauge>::default();
        let height = Family::<Label, HeightGauge>::default();
        let mut registry = <Registry>::default();

        registry.register(
            "sqd_archive_ingest_speed",
            "Blocks processed per second",
            ingest.clone(),
        );

        registry.register(
            "sqd_archive_block_height",
            "Number of the latest processed block",
            height.clone(),
        );

        Self {
            ingest,
            height,
            registry,
        }
    }

    pub fn record_download_speed(&self, blocks_per_second: f64) {
        self.ingest
            .get_or_create(&Label {
                kind: LabelKind::Download,
            })
            .set(blocks_per_second);
    }

    pub fn record_write_speed(&self, blocks_per_second: f64) {
        self.ingest
            .get_or_create(&Label {
                kind: LabelKind::Write,
            })
            .set(blocks_per_second);
    }

    pub fn record_download_height(&self, height: u32) {
        self.height
            .get_or_create(&Label {
                kind: LabelKind::Download,
            })
            .set(i64::from(height));
    }

    pub fn record_write_height(&self, height: u32) {
        self.height
            .get_or_create(&Label {
                kind: LabelKind::Write,
            })
            .set(i64::from(height));
    }

    pub fn record_chain_height(&self, height: u32) {
        self.height
            .get_or_create(&Label {
                kind: LabelKind::Chain,
            })
            .set(i64::from(height));
    }

    pub fn encode(&self) -> Result<String> {
        let mut buf = String::new();

        encode(&mut buf, &self.registry).map_err(Error::EncodeMetrics)?;

        Ok(buf)
    }
}

impl Default for IngestMetrics {
    fn default() -> Self {
        Self::new()
    }
}
