use crate::{Error, Result};
use futures::channel::oneshot;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use threadpool::ThreadPool as ThreadPoolImpl;

pub struct ThreadPool {
    inner: Arc<Mutex<ThreadPoolImpl>>,
    current_num_queries: AtomicUsize,
    max_num_queries: usize,
}

impl ThreadPool {
    pub fn new(max_num_queries: usize, max_jobs_per_query: usize) -> ThreadPool {
        let inner = ThreadPoolImpl::new(max_num_queries * max_jobs_per_query);
        let inner = Arc::new(Mutex::new(inner));

        ThreadPool {
            inner,
            current_num_queries: AtomicUsize::new(0),
            max_num_queries,
        }
    }

    pub async fn spawn<F, T>(&self, func: F) -> Result<T>
    where
        F: 'static + FnOnce() -> T + Send,
        T: 'static + Send,
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

        self.inner.lock().unwrap().execute(move || {
            tx.send(func()).ok().unwrap();
        });

        let res = rx.await.unwrap();

        self.current_num_queries.fetch_sub(1, Ordering::SeqCst);

        Ok(res)
    }

    pub fn spawn_aux<F, T>(&self, func: F)
    where
        F: 'static + FnOnce() -> T + Send,
        T: 'static + Send,
    {
        self.inner.lock().unwrap().execute(|| {
            func();
        });
    }
}
