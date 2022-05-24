use crate::Error;
use std::future::Future;

pub async fn retry<F, T, Fut>(
    num_tries: usize,
    secs_between_tries: u64,
    mut action: F,
) -> Result<T, Vec<Error>>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, Error>>,
{
    let mut errs = Vec::new();
    for _ in 0..num_tries {
        match action().await {
            Ok(r) => return Ok(r),
            Err(e) => {
                eprintln!("failed to run operation: {:#?}, retrying", e);
                tokio::time::sleep(std::time::Duration::from_secs(secs_between_tries)).await;
                errs.push(e);
            }
        }
    }

    Err(errs)
}
