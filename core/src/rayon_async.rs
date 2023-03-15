use tokio::sync::oneshot;

pub async fn spawn<F, T>(func: F) -> T
where
    F: 'static + FnOnce() -> T + Send,
    T: 'static + Send + Sync,
{
    let (tx, rx) = oneshot::channel();

    rayon::spawn(move || {
        let res = func();
        tx.send(res).ok().unwrap();
    });

    rx.await.unwrap()
}
