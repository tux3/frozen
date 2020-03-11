use crate::box_result::BoxResult;
use crate::crypto::{create_secretstream, Key};
use crate::stream::next_stream_bytes;
use bytes::Bytes;
use futures::task::{Context, Poll};
use futures::{Stream, StreamExt};
use sodiumoxide::crypto::secretstream::Tag::Message;
use sodiumoxide::crypto::secretstream::{Header, Push, Stream as SecretStream};
use std::pin::Pin;
use tokio::sync::mpsc;
use tokio::task::block_in_place;

pub struct EncryptionStream {
    output: mpsc::Receiver<BoxResult<Bytes>>,
    stream_lower_bound: usize,
}

impl EncryptionStream {
    pub fn new(input: Box<dyn Stream<Item = BoxResult<Bytes>> + Send + Sync>, key: &Key) -> Self {
        let stream_lower_bound = input.size_hint().0;
        let (send, recv) = mpsc::channel(super::CHUNK_BUFFER_COUNT);

        let (secret_stream, header) = create_secretstream(key);

        tokio::task::spawn(Self::process(input.into(), secret_stream, header, send));
        Self {
            output: recv,
            stream_lower_bound,
        }
    }

    async fn process(
        mut input_stream: Pin<Box<dyn Stream<Item = BoxResult<Bytes>> + Send + Sync>>,
        mut secret_stream: SecretStream<Push>,
        secret_stream_header: Header,
        mut sender: mpsc::Sender<BoxResult<Bytes>>,
    ) {
        // We concat the header with the first encrypted chunk, it'd be too small just by itself
        if let Some(input) = next_stream_bytes(&mut input_stream, &mut sender).await {
            let Header(header_data) = secret_stream_header;
            let mut first_chunk = header_data.to_vec();

            let encrypted = block_in_place(|| secret_stream.push(&input, None, Message).unwrap());

            first_chunk.extend_from_slice(&encrypted);
            if sender.send(Ok(Bytes::from(first_chunk))).await.is_err() {
                return;
            }
        }

        while let Some(input) = next_stream_bytes(&mut input_stream, &mut sender).await {
            let encrypted = block_in_place(|| secret_stream.push(&input, None, Message).unwrap());
            if sender.send(Ok(Bytes::from(encrypted))).await.is_err() {
                return;
            }
        }
    }
}

impl Stream for EncryptionStream {
    type Item = BoxResult<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.output.poll_next_unpin(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.stream_lower_bound, None)
    }
}
