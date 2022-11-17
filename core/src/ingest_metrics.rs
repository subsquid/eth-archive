use crate::{Error, Result};
use core::sync::atomic::{AtomicU32, AtomicU64};
use prometheus_client::encoding::text::{encode, Encode};
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge as GaugeImpl;
use prometheus_client::registry::Registry;

type HeightGauge = GaugeImpl<u32, AtomicU32>;
type IngestGauge = GaugeImpl<f64, AtomicU64>;

pub struct IngestMetrics {
    ingest: Family<Label, IngestGauge>,
    height: Family<Label, HeightGauge>,
    registry: Registry,
}

#[derive(Clone, Hash, PartialEq, Eq, Encode)]
enum LabelKind {
    Download,
    Write,
    Chain,
}

#[derive(Clone, Hash, PartialEq, Eq, Encode)]
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
            Box::new(ingest.clone()),
        );

        registry.register(
            "sqd_archive_block_height",
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
            .set(height);
    }

    pub fn record_write_height(&self, height: u32) {
        self.height
            .get_or_create(&Label {
                kind: LabelKind::Write,
            })
            .set(height);
    }

    pub fn record_chain_height(&self, height: u32) {
        self.height
            .get_or_create(&Label {
                kind: LabelKind::Chain,
            })
            .set(height);
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
