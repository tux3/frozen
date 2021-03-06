mod compression_stream;
pub use compression_stream::*;
mod decompression_stream;
pub use decompression_stream::*;

mod encryption_stream;
pub use encryption_stream::*;
mod decryption_stream;
pub use decryption_stream::*;

mod hashed_stream;
pub use hashed_stream::*;

mod simple_bytes_stream;
pub use simple_bytes_stream::*;

use bytes::Bytes;
use eyre::Result;
use futures::stream::Fuse;
use futures::{Stream, StreamExt};
use std::pin::Pin;
use tokio::sync::mpsc;

/// Size of a byte stream's chunks (must be above B2's 5MB minimum part size)
pub const STREAMS_CHUNK_SIZE: usize = 16 * 1024 * 1024;
/// Max pending chunks that a stream will buffer
pub const CHUNK_BUFFER_COUNT: usize = 1;

type AsyncStreamBox<T> = Pin<Box<dyn Stream<Item = Result<T>> + Sync + Send>>;

/// This returns the next buffer from the stream, or None. Reports errors to the sender.
async fn next_stream_bytes<T>(
    input_stream: &mut Pin<Box<dyn Stream<Item = Result<Bytes>> + Send + Sync>>,
    sender: &mut mpsc::Sender<Result<T>>,
) -> Option<Bytes> {
    match input_stream.next().await {
        Some(Err(err)) => {
            let _ = sender.send(Err(err)).await;
            None
        }
        Some(Ok(input)) => Some(input),
        None => None,
    }
}

/// This reads and returns a buffer up to the desired size (or smaller on EOF)
/// Returns None when there is nothing left to read. Reports errors to the sender.
async fn next_stream_bytes_chunked(
    input_stream: &mut Fuse<impl Stream<Item = Result<Bytes>> + Unpin>,
    next_buf: &mut Vec<u8>,
    desired: usize,
    sender: &mut mpsc::Sender<Result<Bytes>>,
) -> Option<Bytes> {
    if next_buf.len() >= desired {
        let new_next = next_buf[desired..].to_vec();
        next_buf.truncate(desired);
        next_buf.shrink_to_fit();
        return Some(std::mem::replace(next_buf, new_next).into());
    }

    loop {
        let input = match input_stream.next().await {
            Some(Err(err)) => {
                let _ = sender.send(Err(err)).await;
                break None;
            }
            Some(Ok(input)) => input,
            // Note how we return a last Some after None, hence why we need a Fuse<> input stream
            None if !next_buf.is_empty() => return Some(std::mem::take(next_buf).into()),
            None => break None,
        };

        let remaining = desired.saturating_sub(next_buf.len());
        let available = remaining.min(input.len());
        next_buf.extend_from_slice(&input[..available]);

        if available == remaining {
            debug_assert_eq!(next_buf.len(), desired);
            let new_next = input[available..].to_vec();
            break Some(std::mem::replace(next_buf, new_next).into());
        }
    }
}
