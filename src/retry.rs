use crate::Error;
use std::future::Future;

pub async fn retry<F, T, Fut>(mut action: F) -> Result<T, Vec<Error>>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, Error>>,
{
    let mut errs = Vec::new();
    for _ in 0..3 {
        match action().await {
            Ok(r) => return Ok(r),
            Err(e) => {
                tracing::error!("failed to run operation: {}, retrying", e);
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                errs.push(e);
            }
        }
    }

    Err(errs)
}
