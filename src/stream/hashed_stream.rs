use crate::box_result::BoxResult;
use crate::crypto::sha1_string;
use bytes::Bytes;
use futures::task::{Context, Poll};
use futures::{Stream, StreamExt};
use tokio::macros::support::Pin;
use tokio::sync::mpsc;
use tokio::task::block_in_place;

pub struct HashedStream {
    output: mpsc::Receiver<BoxResult<(Bytes, String)>>,
    stream_lower_bound: usize,
}

impl HashedStream {
    pub fn new(input: Box<dyn Stream<Item = BoxResult<Bytes>> + Send + Sync>) -> Self {
        let stream_lower_bound = input.size_hint().0;
        let (send, recv) = mpsc::channel(super::CHUNK_BUFFER_COUNT);
        tokio::task::spawn(Self::process(input.into(), send));
        Self {
            output: recv,
            stream_lower_bound,
        }
    }

    async fn process(
        mut input_stream: Pin<Box<dyn Stream<Item = BoxResult<Bytes>> + Send + Sync>>,
        mut sender: mpsc::Sender<BoxResult<(Bytes, String)>>,
    ) {
        while let Some(input) = input_stream.next().await {
            match input {
                Err(err) => {
                    let _ = sender.send(Err(err)).await;
                    break;
                }
                Ok(input) => {
                    let sha1 = block_in_place(|| sha1_string(&input));
                    if sender.send(Ok((input, sha1))).await.is_err() {
                        return;
                    }
                }
            }
        }
    }
}

impl Stream for HashedStream {
    type Item = BoxResult<(Bytes, String)>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.output.poll_next_unpin(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.stream_lower_bound, None)
    }
}
