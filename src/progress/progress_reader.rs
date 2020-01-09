use futures::{stream::Stream, task::Poll};
use hyper::body::Bytes;
use std::cmp;
use std::error::Error;
use std::io::{self, Read};
use std::pin::Pin;
use std::task::Context;

const DATA_READER_MIN_CHUNK_SIZE: usize = 4 * 1024;
const DATA_READER_MAX_CHUNK_SIZE: usize = 4 * 1024 * 1024;

pub struct ProgressDataReader {
    data: Bytes,
    pos: usize,
}

impl ProgressDataReader {
    pub fn new(data: Vec<u8>) -> ProgressDataReader {
        ProgressDataReader {
            data: data.into(),
            pos: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.data
    }
}

impl Clone for ProgressDataReader {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
            pos: self.pos,
        }
    }
}

impl Stream for ProgressDataReader {
    type Item = Result<Bytes, Box<dyn Error + Sync + Send + 'static>>;

    fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let chunk_size = clamp::clamp(
            DATA_READER_MIN_CHUNK_SIZE,
            self.data.len() / 200,
            DATA_READER_MAX_CHUNK_SIZE,
        );
        let read_size = cmp::min(chunk_size, self.len() - self.pos);
        let chunk_slice = self.data.slice(self.pos..self.pos + read_size);
        self.pos += read_size;

        Poll::Ready(Some(Ok(chunk_slice.into())))
    }
}

impl Read for ProgressDataReader {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, io::Error> {
        let read_size = cmp::min(buf.len(), self.len() - self.pos);
        let (_, remaining) = self.data.split_at(self.pos);
        let (target, _) = remaining.split_at(read_size);
        buf[..read_size].copy_from_slice(target);

        self.pos += read_size;
        Ok(read_size)
    }
}
