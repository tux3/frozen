use std::vec::Vec;
use std::io::{self, stdout, Write, Read, ErrorKind};
use std::error::Error;
use std::cmp;
use futures::channel::mpsc::Sender;
use pretty_bytes::converter::convert;
use bytes::Bytes;
use futures::{stream::Stream, Poll};
use hyper::Chunk;
use ignore_result::Ignore;
use crate::net::progress_thread;
use super::vt100::*;
use std::pin::Pin;
use futures::task::Context;

#[derive(Debug)]
pub enum Progress {
    Started(String),
    Warning(String),
    Error(String),
    Transferred(String),
    Deleted(String),
    Deleting,
    Terminated,
    Downloading(u8),
    Uploading(u8, u64),
    Compressing(u8),
    Decompressing(u8),
    Encrypting(u8),
    Decrypting(u8),
}

const DATA_READER_MIN_CHUNK_SIZE: usize = 4*1024;
const DATA_READER_MAX_CHUNK_SIZE: usize = 4*1024*1024;

pub struct ProgressDataReader {
    data: Bytes,
    pos: usize,
    tx_progress: Option<Sender<Progress>>,
}

impl ProgressDataReader {
    pub fn new(data: Vec<u8>, tx_progress: Option<Sender<Progress>>) -> ProgressDataReader {
        ProgressDataReader {
            data: data.into(),
            pos: 0,
            tx_progress,
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
            tx_progress: self.tx_progress.clone(),
        }
    }
}

impl Stream for ProgressDataReader {
    type Item = Result<Chunk, Box<dyn Error + Sync + Send + 'static>>;

    fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>,) -> Poll<Option<Self::Item>> {
        let chunk_size = clamp::clamp(DATA_READER_MIN_CHUNK_SIZE,
                                      self.data.len() / 200,
                                      DATA_READER_MAX_CHUNK_SIZE);
        let read_size = cmp::min(chunk_size, self.len()-self.pos);
        let chunk_slice = self.data.slice(self.pos, self.pos+read_size);
        self.pos += read_size;

        if self.tx_progress.is_some() {
            let progress = Progress::Uploading((self.pos * 100 / self.len()) as u8, self.len() as u64);
            self.tx_progress.as_mut().unwrap().try_send(progress).ignore();
        }

        Poll::Ready(Some(Ok(chunk_slice.into())))
    }
}

impl Read for ProgressDataReader {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, io::Error> {
        let read_size = cmp::min(buf.len(), self.len()-self.pos);
        let (_, remaining) = self.data.split_at(self.pos);
        let (target, _) = remaining.split_at(read_size);
        buf[..read_size].copy_from_slice(target);

        self.pos += read_size;
        if self.tx_progress.is_some() {
            let progress = Progress::Uploading((self.pos * 100 / self.len()) as u8, self.len() as u64);
            if self.tx_progress.as_mut().unwrap().try_send(progress).is_err() {
                return Err(io::Error::new(ErrorKind::Other, "Receiving thread seems gone"));
            }
        }
        Ok(read_size)
    }
}

/// Call once before using the progress output functions
pub fn start_output(num_threads: usize) {
    for thread_id in 0..num_threads {
        println!("{} Waiting to transfer...", num_threads-thread_id);
    }
}

/// This makes use of VT100, so don't mix with regular print functions
pub fn progress_output(verbose: bool, progress: &Progress, thread_id: usize, num_threads: usize) {
    use self::Progress::*;

    let off = thread_id+1;
    match progress {
        Started(str) => rewrite_at(off, VT100::StyleActive, &format!("Started \t\t\t{}", str)),
        Uploading(n, s) => write_at(off, VT100::StyleActive,
                                            &format!("Uploaded {}% of {}", n, convert(*s as f64))),
        Downloading(_n) => write_at(off, VT100::StyleActive,  "Downloading        "),
        Compressing(_) => write_at(off, VT100::StyleActive,   "Compressing        "),
        Decompressing(_) => write_at(off, VT100::StyleActive, "Decompressing      "),
        Encrypting(_) => write_at(off, VT100::StyleActive,    "Encrypting         "),
        Decrypting(_) => write_at(off, VT100::StyleActive,    "Decrypting         "),
        Deleting => write_at(off, VT100::StyleActive,         "Deleting           "),
        Warning(str) => {
            insert_at(num_threads, VT100::StyleWarning, &format!("Warning: {}", str));
        },
        Error(str) => {
            rewrite_at(off, VT100::StyleActive,               "Done               ");
            insert_at(num_threads, VT100::StyleError, &format!("Error: {}", str));
        },
        Transferred(str) => {
            rewrite_at(off, VT100::StyleActive,               "Done               ");
            if verbose {
                insert_at(num_threads, VT100::StyleReset, &format!("Transferred \t\t\t{}", str));
            }
        },
        Deleted(str) => {
            rewrite_at(off, VT100::StyleActive,               "Done               ");
            if verbose {
                insert_at(num_threads, VT100::StyleReset, &format!("Deleted     \t\t\t{}", str));
            }
        },
        Terminated => {
            remove_at(off);
        }
    };
}

pub fn flush() {
    stdout().flush().unwrap();
}

/// Receives and displays progress information. Removes dead threads from the list.
pub async fn handle_progress<T: progress_thread::ProgressThread>(verbose: bool, threads: &mut Vec<T>) {
    for thread_id in (0..threads.len()).rev() {
        let mut delete_later = false;
        {
            let num_threads = threads.len();
            let thread = &mut threads[thread_id];
            loop {
                let progress = match thread.progress_rx().try_next() {
                    Err(_) => break,
                    Ok(None) => { delete_later = true; break },
                    Ok(Some(progress)) => progress,
                };

                if let Progress::Terminated = progress {
                    delete_later = true;
                }
                progress_output(verbose, &progress, thread_id, num_threads)
            }
        }
        if delete_later {
            threads.remove(thread_id);
        }
    }
    flush();
}