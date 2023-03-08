use crate::{Error, Result};
use rayon::{ThreadPool as RayonThreadPool, ThreadPoolBuilder};
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use tokio::sync::oneshot;

pub struct ThreadPool {
    inner: RayonThreadPool,
    current_num_queries: AtomicUsize,
    max_num_queries: usize,
}

impl ThreadPool {
    pub fn new(max_num_queries: usize) -> ThreadPool {
        ThreadPool {
            inner: ThreadPoolBuilder::new()
                .num_threads(max_num_queries)
                .build()
                .unwrap(),
            current_num_queries: AtomicUsize::new(0),
            max_num_queries,
        }
    }

    pub async fn spawn<F, T>(&self, func: F) -> Result<T>
    where
        F: 'static + FnOnce() -> T + Send,
        T: 'static + Send + Sync,
    {
        let max_num_queries = self.max_num_queries;
        if self
            .current_num_queries
            .fetch_update(
                Ordering::SeqCst,
                Ordering::SeqCst,
                |current_num_queries: usize| {
                    if current_num_queries < max_num_queries {
                        Some(current_num_queries + 1)
                    } else {
                        None
                    }
                },
            )
            .is_err()
        {
            return Err(Error::MaxNumberOfQueriesReached);
        }

        let (tx, rx) = oneshot::channel();

        self.inner.spawn(move || {
            tx.send(func()).ok().unwrap();
        });

        Ok(rx.await.unwrap())
    }
}
