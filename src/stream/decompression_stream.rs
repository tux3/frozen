use crate::box_result::BoxResult;
use crate::stream::next_stream_bytes;
use bytes::Bytes;
use futures::task::{Context, Poll};
use futures::{Stream, StreamExt};
use std::io::Write;
use std::pin::Pin;
use tokio::sync::mpsc;
use tokio::task::block_in_place;

/// This "stream" takes a compressed input stream, but writes its output directly to an impl Write
pub struct DecompressionStream {
    output: mpsc::Receiver<BoxResult<()>>,
}

impl DecompressionStream {
    pub fn new(
        input: Box<dyn Stream<Item = BoxResult<Bytes>> + Send + Sync>,
        output: impl Write + Send + 'static,
    ) -> Self {
        let (send, recv) = mpsc::channel(super::CHUNK_BUFFER_COUNT);

        tokio::task::spawn(Self::process(input.into(), Box::new(output), send));
        Self { output: recv }
    }

    async fn process(
        mut input_stream: Pin<Box<dyn Stream<Item = BoxResult<Bytes>> + Send + Sync>>,
        output: Box<dyn Write + Send>,
        mut sender: mpsc::Sender<BoxResult<()>>,
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
    type Item = BoxResult<()>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.output.poll_next_unpin(cx)
    }
}
