use crate::stream::{next_stream_bytes, AsyncStreamBox};
use async_stream::stream;
use bytes::Bytes;
use eyre::Result;
use futures::task::{Context, Poll};
use futures::{Stream, StreamExt};
use std::io::Write;
use std::pin::Pin;
use tokio::sync::mpsc;
use tokio::task::block_in_place;

/// This "stream" takes a compressed input stream, but writes its output directly to an impl Write
pub struct DecompressionStream {
    output: AsyncStreamBox<()>,
}

impl DecompressionStream {
    pub fn new(
        input: Box<dyn Stream<Item = Result<Bytes>> + Send + Sync>,
        output: impl Write + Send + 'static,
    ) -> Self {
        let (send, mut recv) = mpsc::channel(super::CHUNK_BUFFER_COUNT);

        tokio::task::spawn(Self::process(input.into(), Box::new(output), send));
        let stream_recv = Box::pin(stream! {
            while let Some(item) = recv.recv().await {
                yield item;
            }
        });
        Self { output: stream_recv }
    }

    async fn process(
        mut input_stream: Pin<Box<dyn Stream<Item = Result<Bytes>> + Send + Sync>>,
        output: Box<dyn Write + Send>,
        mut sender: mpsc::Sender<Result<()>>,
    ) {
        let mut decoder = zstd::stream::write::Decoder::new(output).unwrap();

        while let Some(input) = next_stream_bytes(&mut input_stream, &mut sender).await {
            block_in_place(|| {
                decoder.write_all(&input).unwrap();
            });
            if sender.send(Ok(())).await.is_err() {
                return;
            }
        }

        decoder.flush().unwrap();
    }
}

impl Stream for DecompressionStream {
    type Item = Result<()>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.output.poll_next_unpin(cx)
    }
}
