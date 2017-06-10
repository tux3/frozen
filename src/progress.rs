use vt100::*;
use std::io::{stdout, Write, Read, Error, ErrorKind};
use std::cmp;
use std::sync::mpsc::Sender;
use pretty_bytes::converter::convert;
use net::progress_thread;

pub enum Progress {
    Started(String),
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

pub struct ProgressDataReader {
    data: Vec<u8>,
    pos: usize,
    tx_progress: Option<Sender<Progress>>,
}

impl ProgressDataReader {
    pub fn new(data: Vec<u8>, tx_progress: Option<Sender<Progress>>) -> ProgressDataReader {
        ProgressDataReader {
            data: data,
            pos: 0,
            tx_progress: tx_progress,
        }
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn as_slice(&self) -> &[u8] {
        self.data.as_slice()
    }
}

impl Read for ProgressDataReader {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, Error> {
        let read_size = cmp::min(buf.len(), self.len()-self.pos);
        let (_, remaining) = self.data.split_at(self.pos);
        let (target, _) = remaining.split_at(read_size);
        buf[..read_size].copy_from_slice(target);

        self.pos += read_size;
        if self.tx_progress.is_some() {
            let progress = (self.pos*100/self.len()) as u8;
            if self.tx_progress.as_ref().unwrap()
                            .send(Progress::Uploading(progress, self.len() as u64)).is_err() {
                return Err(Error::new(ErrorKind::Other, "Receiving thread seems gone"));
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
pub fn progress_output(progress: &Progress, thread_id: usize, num_threads: usize) {
    use self::Progress::*;

    let off = thread_id+1;
    match *progress {
        Started(ref str) => rewrite_at(off, VT100::StyleActive, &format!("Started \t\t\t{}", str)),
        Uploading(ref n, ref s) => write_at(off, VT100::StyleActive,
                                            &format!("Uploaded {}% of {}", n, convert(*s as f64))),
        Downloading(ref n) => write_at(off, VT100::StyleActive, &format!("Downloaded     {}%", n)),
        Compressing(_) => write_at(off, VT100::StyleActive,   "Compressing        "),
        Decompressing(_) => write_at(off, VT100::StyleActive, "Decompressing      "),
        Encrypting(_) => write_at(off, VT100::StyleActive,    "Encrypting         "),
        Decrypting(_) => write_at(off, VT100::StyleActive,    "Decrypting         "),
        Deleting => write_at(off, VT100::StyleActive,      "Deleting           "),
        Error(ref str) => {
            rewrite_at(off, VT100::StyleActive,               "Done               ");
            insert_at(num_threads, VT100::StyleError, &format!("Error: {}", str));
        },
        Transferred(ref str) => {
            rewrite_at(off, VT100::StyleActive,               "Done               ");
            insert_at(num_threads, VT100::StyleReset, &format!("Transferred \t\t\t{}", str));
        },
        Deleted(ref str) => {
            rewrite_at(off, VT100::StyleActive,               "Done               ");
            insert_at(num_threads, VT100::StyleReset, &format!("Deleted     \t\t\t{}", str));
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
pub fn handle_progress<T: progress_thread::ProgressThread>(threads: &mut Vec<T>) {
    let mut num_threads = threads.len();
    let mut thread_id = 0;
    while thread_id < num_threads {
        let mut delete_later = false;
        {
            let thread = &threads[thread_id];
            loop {
                let progress = thread.progress_rx().try_recv();
                if progress.is_err() {
                    break;
                }
                let progress = progress.unwrap();
                if let Progress::Terminated = progress {
                    delete_later = true;
                }
                progress_output(&progress, thread_id, num_threads);
            }
        }
        if delete_later {
            threads.remove(thread_id);
            num_threads -= 1;
        }

        thread_id += 1;
    }
    flush();
}