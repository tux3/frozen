use crate::box_result::BoxResult;
use bytes::Bytes;
use futures::task::{Context, Poll};
use futures::Stream;
use tokio::macros::support::Pin;

pub struct SimpleBytesStream {
    bytes: Bytes,
    done: bool,
}

impl SimpleBytesStream {
    pub fn new(bytes: Bytes) -> Self {
        Self { bytes, done: false }
    }
}

impl Stream for SimpleBytesStream {
    type Item = BoxResult<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.done {
            Poll::Ready(None)
        } else {
            self.done = true;
            Poll::Ready(Some(Ok(self.bytes.clone())))
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        // We always return exactly one chunk
        (1, Some(1))
    }
}
