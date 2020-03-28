use crate::box_result::BoxResult;
use crate::stream::STREAMS_CHUNK_SIZE;
use bytes::Bytes;
use futures::task::{Context, Poll};
use futures::{Stream, StreamExt};
use std::io::Read;
use std::pin::Pin;
use tokio::sync::{mpsc, oneshot};
use tokio::task::block_in_place;

pub struct CompressionStream {
    output: mpsc::Receiver<BoxResult<Bytes>>,
    stream_lower_bound: usize,
}

impl CompressionStream {
    pub async fn new(input: impl Read + Send + 'static, compress_level: i32) -> Self {
        let (send, recv) = mpsc::channel(super::CHUNK_BUFFER_COUNT);
        let (lower_bound_send, lower_bound_recv) = oneshot::channel();

        tokio::task::spawn(Self::process(Box::new(input), compress_level, send, lower_bound_send));
        Self {
            output: recv,
            stream_lower_bound: lower_bound_recv.await.unwrap(),
        }
    }

    async fn process(
        input: Box<dyn Read + Send>,
        compress_level: i32,
        mut sender: mpsc::Sender<BoxResult<Bytes>>,
        lower_bound_send: oneshot::Sender<usize>,
    ) {
        let mut encoder = zstd::stream::read::Encoder::new(input, compress_level).unwrap();

        let mut lower_bound_send = Some(lower_bound_send);
        let mut chunks_count = 0;

        let mut pos = 0usize;
        let mut buf = vec![0u8; STREAMS_CHUNK_SIZE].into_boxed_slice();
        loop {
            let read_count = match block_in_place(|| encoder.read(&mut buf[pos..])) {
                Err(err) => {
                    let _ = sender.send(Err(err.into())).await;
                    break;
                }
                Ok(n) => n,
            };

            let at_end = read_count == 0;
            pos += read_count;

            if pos == STREAMS_CHUNK_SIZE || at_end {
                chunks_count += 1;
                if chunks_count == 2 {
                    if let Some(sender) = lower_bound_send.take() {
                        sender.send(chunks_count).unwrap()
                    }
                }
                let mut bytes = buf.into_vec();
                bytes.truncate(pos);
                if sender.send(Ok(bytes.into())).await.is_err() {
                    break;
                }
                buf = vec![0u8; STREAMS_CHUNK_SIZE].into_boxed_slice();
                pos = 0;
                if at_end {
                    break;
                }
            }
        }

        if let Some(sender) = lower_bound_send.take() {
            sender.send(chunks_count).unwrap();
        }
    }
}

impl Stream for CompressionStream {
    type Item = BoxResult<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.output.poll_next_unpin(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.stream_lower_bound, None)
    }
}
