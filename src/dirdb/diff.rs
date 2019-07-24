use std::error::Error;
use std::pin::Pin;
use std::cell::Cell;
use std::sync::{Mutex, Arc};
use std::task::Context;
use futures::channel::mpsc::{channel, Sender, Receiver};
use futures::future::{FutureExt, TryFutureExt, BoxFuture};
use futures::{Stream, StreamExt, Poll, Future};
use self::files::FileDiffStream;
use super::{DirDB, DirStat, FileStat};
use crate::data::root::BackupRoot;
use crate::net::b2::B2;
use crate::crypto::Key;

mod dirs;
mod files;
pub use files::FileDiff;

/// Use this struct to start diffing folders and to receive `FileDiff`s
pub struct DirDiff {
    diff_stream: FileDiffStream,
    receiver: Receiver<FileDiff>,
    sender: Mutex<Cell<Option<Sender<FileDiff>>>>,
    pessimistic_dirdb: Option<DirDB>,
}

impl DirDiff {
    pub fn new<'a>(root: &'a BackupRoot, b2: &'a B2, local: &'a DirDB, remote: Option<DirDB>) -> Result<DirDiff, Box<dyn Error>> {
        let (tx_file, rx_file) = channel(16);
        let list_all_fut = files::diff_files_at(root, b2, tx_file.clone(), "", &local.root).boxed::<'a>();
        let remote = match remote {
            Some(remote) => remote,
            None => return Ok(DirDiff {
                diff_stream: FileDiffStream::new(),
                receiver: rx_file,
                sender: Mutex::new(Cell::new(Some(tx_file))),
                pessimistic_dirdb: None,
            }),
        };

        let pessimistic_dirdb = DirDB {
            root: dirs::merge_dirstats_pessimistic(&local.root, &remote.root),
        };

        Ok(DirDiff {
            diff_stream: FileDiffStream::new(),
            receiver: rx_file,
            sender: Mutex::new(Cell::new(Some(tx_file))),
            pessimistic_dirdb: Some(pessimistic_dirdb),
        })
    }

    pub fn get_pessimistic_dirdb_data(&self, key: &Key) -> Result<Option<Vec<u8>>, Box<dyn Error>> {
        self.pessimistic_dirdb.as_ref().map(|db| db.to_packed(key)).transpose()
    }

    pub async fn start_diff_remote_files(&mut self, root: Arc<BackupRoot>, b2: Arc<B2>) {
        let tx_file = self.sender.lock().unwrap().take().expect("Cannot start multiple diffs");
        // TODO: Swap list_requests w/ empty vec?
        //  > Maybe since we impl Stream, we should not expose a receiver at all, instead we could properly implement poll_next() to go through our list_requests
        //    We can go back to putting all the list request futures in a vec and/or joining them, the goal being to run the network requests in parallel
        //  => How do we forward multiple results from each list request (on different threads!) to the stream output? I guess we can keep the sender/receiver internally!
        //  > We can probably eliminate start_diff_remote_files from the API and just create the futures in the ctor (but don't await them)
        //  > Can't we make each of the futures we have Streams, and merge them into one?
        //    Each list request would be started as its own Stream and we just have to poll it without bothering with send/recv

//        let list_requests = std::mem::replace(&mut self.list_requests, Vec::new());
//        list_requests.into_iter().map(|(prefix, stat)| {
//            tokio::spawn(files::diff_files_at(root.clone(), b2.clone(), tx_file.clone(), &prefix.clone(), stat.clone()).unwrap_or_else(|_| ()))
//        });
    }
}

impl Stream for DirDiff {
    type Item = FileDiff;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context) -> Poll<Option<Self::Item>> {
        self.diff_stream.poll_next_unpin(context)
    }
}
