use crate::box_result::BoxResult;
use futures::future::{select, Either, FutureExt};
use std::future::Future;
use tokio::signal::ctrl_c;

/// Runs the future, but interrupts it and returns Err if Ctrl+C is pressed
pub async fn interruptible(fut: impl Future<Output = BoxResult<()>>) -> BoxResult<()> {
    let int_fut = ctrl_c().boxed_local();
    let fut = fut.boxed_local();
    match select(fut, int_fut).await {
        Either::Left((fut_result, _int_fut)) => fut_result,
        Either::Right((Ok(()), _fut)) => Err(From::from("Interrupted by Ctrl+C")),
        Either::Right((Err(_), fut)) => fut.await,
    }
}
