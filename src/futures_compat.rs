use futures::compat::Compat;
use futures::FutureExt;
use std::future::Future;

/// Replacement for tokio::run compatible with futures 0.3
pub fn tokio_run<F: Future<Output=()> + Send + 'static>(future: F) {
    tokio::run(Compat::new(Box::pin(
        future.map(|()| -> Result<(), ()> { Ok(()) })
    )));
    tokio::run(Compat::new(futures::future::lazy(|_| Ok(()))));
}

/// Replacement for tokio::spawn compatible with futures 0.3
pub fn tokio_spawn<F: Future<Output=()> + Send + 'static>(future: F) {
    tokio::spawn(Compat::new(Box::pin(
        future.map(|()| -> Result<(), ()> { Ok(()) })
    )));
}