use crate::box_result::BoxResult;
use crate::crypto::{open_secretstream, Key};
use crate::stream::{next_stream_bytes_chunked, STREAMS_CHUNK_SIZE};
use bytes::Bytes;
use futures::task::{Context, Poll};
use futures::{Stream, StreamExt};
use hyper::Body;
use sodiumoxide::crypto::secretstream::{Tag, HEADERBYTES};
use std::pin::Pin;
use tokio::sync::mpsc;
use tokio::task::block_in_place;

pub struct DecryptionStream {
    output: mpsc::Receiver<BoxResult<Bytes>>,
}

impl DecryptionStream {
    pub fn new(input: Body, key: &Key) -> Self {
        let (send, recv) = mpsc::channel(super::CHUNK_BUFFER_COUNT);

        tokio::task::spawn(Self::process(input, key.clone(), send));
        Self { output: recv }
    }

    async fn process(input: Body, key: Key, mut sender: mpsc::Sender<BoxResult<Bytes>>) {
        let mut buf = Vec::new();
        let mut input = input.fuse();

        let mut secret_stream = match next_stream_bytes_chunked(&mut input, &mut buf, HEADERBYTES, &mut sender).await {
            Some(header) if header.len() == HEADERBYTES => open_secretstream(header.as_ref(), &key),
            _ => {
                let _ = sender
                    .send(Err(From::from(
                        "Couldn't decrypt: failed to read secretstream header. Is the data corrupt?",
                    )))
                    .await;
                return;
            }
        };

        while let Some(input) = next_stream_bytes_chunked(&mut input, &mut buf, STREAMS_CHUNK_SIZE, &mut sender).await {
            let (decrypted, tag) = match block_in_place(|| secret_stream.pull(&input, None)) {
                Ok(result) => result,
                Err(()) => {
                    let _ = sender
                        .send(Err(From::from(
                            "Decryption failed: Unknown error in secret_stream.pull()",
                        )))
                        .await;
                    return;
                }
            };
            assert_eq!(tag, Tag::Message);
            if sender.send(Ok(Bytes::from(decrypted))).await.is_err() {
                return;
            }
        }
    }
}

impl Stream for DecryptionStream {
    type Item = BoxResult<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.output.poll_next_unpin(cx)
    }
}
