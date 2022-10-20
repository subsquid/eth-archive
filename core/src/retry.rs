use crate::config::RetryConfig;
use std::future::Future;
use std::time::Duration;

#[derive(Clone, Copy)]
pub struct Retry {
    num_tries: Option<usize>,
    secs_between_tries: Duration,
}

impl Retry {
    pub fn new(cfg: RetryConfig) -> Self {
        Self {
            num_tries: cfg.num_tries,
            secs_between_tries: Duration::from_secs(3),
        }
    }

    pub async fn retry<F, T, Fut, E: std::error::Error>(&self, mut action: F) -> Result<T, Vec<E>>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        if let Some(num_tries) = self.num_tries {
            let mut errs = Vec::new();
            for _ in 0..num_tries {
                match action().await {
                    Ok(r) => return Ok(r),
                    Err(e) => {
                        log::error!("failed to run operation: {}, retrying", e);
                        tokio::time::sleep(self.secs_between_tries).await;
                        errs.push(e);
                    }
                }
            }
            Err(errs)
        } else {
            loop {
                match action().await {
                    Ok(r) => return Ok(r),
                    Err(e) => {
                        log::error!("failed to run operation: {}, retrying", e);
                        tokio::time::sleep(self.secs_between_tries).await;
                    }
                }
            }
        }
    }
}
