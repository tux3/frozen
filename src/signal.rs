use crate::box_result::BoxResult;
use futures::future::{select, Either};
use futures::{FutureExt, StreamExt};
use std::future::Future;
use tokio_net::signal::{ctrl_c, CtrlC};

/// On creation this struct starts catching Ctrl+C (and never stops, even if dropped)
pub struct SignalHandler {
    stream: CtrlC,
}

impl SignalHandler {
    pub fn new() -> BoxResult<Self> {
        Ok(Self { stream: ctrl_c()? })
    }

    /// Runs the future, but interrupts it and returns Err if Ctrl+C is pressed
    pub async fn interruptible(&mut self, fut: impl Future<Output = BoxResult<()>>) -> BoxResult<()> {
        let int_fut = self.stream.next();
        let fut = fut.boxed_local();
        match select(fut, int_fut).await {
            Either::Left((fut_result, _int_fut)) => fut_result,
            Either::Right((Some(()), _fut)) => Err(From::from("Interrupted by Ctrl+C")),
            Either::Right((None, _fut)) => unreachable!("ctrl_c is supposed to be an infinite stream!"),
        }
    }
}
