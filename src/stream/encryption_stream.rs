use crate::crypto::{create_secretstream, Key};
use crate::stream::{next_stream_bytes_chunked, STREAMS_CHUNK_SIZE};
use bytes::Bytes;
use eyre::{eyre, Result};
use futures::task::{Context, Poll};
use futures::{Stream, StreamExt};
use sodiumoxide::crypto::secretstream::{Header, Push, Stream as SecretStream};
use sodiumoxide::crypto::secretstream::{Tag, ABYTES};
use std::pin::Pin;
use tokio::sync::mpsc;
use tokio::task::block_in_place;

pub struct EncryptionStream {
    output: mpsc::Receiver<Result<Bytes>>,
    stream_lower_bound: usize,
}

impl EncryptionStream {
    pub fn new(input: Box<dyn Stream<Item = Result<Bytes>> + Send + Sync>, key: &Key) -> Self {
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
        input_stream: Pin<Box<dyn Stream<Item = Result<Bytes>> + Send + Sync>>,
        mut secret_stream: SecretStream<Push>,
        secret_stream_header: Header,
        mut sender: mpsc::Sender<Result<Bytes>>,
    ) {
        let mut buf = Vec::new();
        let mut input = input_stream.fuse();

        // We concat the header with the first encrypted chunk, it'd be too small just by itself
        if let Some(data) = next_stream_bytes_chunked(&mut input, &mut buf, STREAMS_CHUNK_SIZE, &mut sender).await {
            let Header(header_data) = secret_stream_header;
            let mut first_chunk = header_data.to_vec();

            let encrypted_chunk_size = data.len() + ABYTES;
            let size_buf = (encrypted_chunk_size as u64).to_le_bytes();
            debug_assert_eq!(size_buf.len(), std::mem::size_of::<u64>());
            let encrypted_encrypted_chunk_size =
                &mut block_in_place(|| secret_stream.push(&size_buf, None, Tag::Push).unwrap());
            debug_assert_eq!(encrypted_encrypted_chunk_size.len(), size_buf.len() + ABYTES);
            first_chunk.append(encrypted_encrypted_chunk_size);

            let encrypted = &mut block_in_place(|| secret_stream.push(&data, None, Tag::Message).unwrap());
            debug_assert_eq!(encrypted.len(), encrypted_chunk_size);
            drop(data);
            first_chunk.append(encrypted);

            if sender.send(Ok(Bytes::from(first_chunk))).await.is_err() {
                return;
            }
        } else {
            let _ = sender.send(Err(eyre!("No input data, failed to encrypt!"))).await;
            return;
        }

        while let Some(input) = next_stream_bytes_chunked(&mut input, &mut buf, STREAMS_CHUNK_SIZE, &mut sender).await {
            let encrypted = block_in_place(|| secret_stream.push(&input, None, Tag::Message).unwrap());
            debug_assert_eq!(encrypted.len(), input.len() + ABYTES);
            drop(input);
            if sender.send(Ok(Bytes::from(encrypted))).await.is_err() {
                return;
            }
        }
    }
}

impl Stream for EncryptionStream {
    type Item = Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.output.poll_next_unpin(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.stream_lower_bound, None)
    }
}
