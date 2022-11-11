use prometheus_client::encoding::text::Encode;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge as GaugeImpl;
use prometheus_client::registry::Registry;
use core::sync::atomic::AtomicU32;

type Gauge = GaugeImpl<u32, AtomicU32>;

pub struct Metrics {
    ingest: Family<Label, Gauge>,
    height: Family<Label, Gauge>,
    registry: Registry,
}

#[derive(Clone, Hash, PartialEq, Eq, Encode)]
enum Label {
    Download,
    ParquetWrite,
}

impl Metrics {
    pub fn new() -> Self {
        let ingest = Family::<Label, Gauge>::default();
        let height = Family::<Label, Gauge>::default();
        let mut registry = <Registry>::default();

        registry.register(
            "ingest_speed",
            "Blocks ingested per second",
            Box::new(ingest.clone()),
        );

        registry.register(
            "block_height",
            "Number of the latest processed block",
            Box::new(height.clone()),
        );

        Self { ingest, height, registry }
    }

    pub fn record_download_speed(&self, blocks_per_second: u32) {
        self.ingest.get_or_create(&Label::Download).set(blocks_per_second);
    }

    pub fn record_parquet_write_speed(&self, blocks_per_second: u32) {
        self.ingest.get_or_create(&Label::ParquetWrite).set(blocks_per_second);
    }

    pub fn record_download_height(&self, height: u32) {
        self.height.get_or_create(&Label::Download).set(height);
    }

    pub fn record_parquet_write_height(&self, height: u32) {
        self.height.get_or_create(&Label::ParquetWrite).set(height);
    }
}
