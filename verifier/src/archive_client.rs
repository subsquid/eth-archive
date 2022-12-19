use crate::types::ArchiveResponse;
use crate::{Error, Result};
use eth_archive_core::config::IngestConfig;
use eth_archive_core::retry::Retry;
use eth_archive_worker::Query;
use std::sync::Arc;
use std::time::Duration;
use url::Url;

pub struct ArchiveClient {
    http_client: reqwest::Client,
    archive_url: Url,
    ingest_config: IngestConfig,
    retry: Retry,
}

impl ArchiveClient {
    pub fn new(archive_url: Url, ingest_config: IngestConfig, retry: Retry) -> Result<Self> {
        let request_timeout = Duration::from_secs(ingest_config.request_timeout_secs.get());
        let connect_timeout = Duration::from_millis(ingest_config.connect_timeout_ms.get());

        let http_client = reqwest::ClientBuilder::new()
            .gzip(true)
            .timeout(request_timeout)
            .connect_timeout(connect_timeout)
            .build()
            .map_err(Error::BuildHttpClient)?;

        Ok(Self {
            http_client,
            archive_url,
            ingest_config,
            retry,
        })
    }

    async fn send_impl(&self, query: &Query) -> Result<ArchiveResponse> {
        let resp = self
            .http_client
            .post(self.archive_url.clone())
            .json(query)
            .send()
            .await
            .map_err(Error::HttpRequest)?;

        let resp_status = resp.status();
        if !resp_status.is_success() {
            let body = resp.text().await.ok();
            return Err(Error::RpcResponseStatus(resp_status.as_u16(), body));
        }

        let resp_body = resp.json().await.map_err(Error::RpcResponseParse)?;

        Ok(resp_body)
    }

    pub async fn send(self: Arc<Self>, query: Arc<Query>) -> Result<ArchiveResponse> {
        self.retry
            .retry(|| {
                let client = self.clone();
                let query = query.clone();
                async move { client.send_impl(&query).await }
            })
            .await
            .map_err(Error::Retry)
    }
}
