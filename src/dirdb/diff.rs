use std::error::Error;
use std::pin::Pin;
use std::cell::Cell;
use std::sync::Mutex;
use std::task::Context;
use futures::channel::mpsc::{channel, Sender, Receiver};
use futures::future::FutureExt;
use futures::{Stream, Poll};
use super::{DirDB, DirStat, FileStat};
use crate::data::root::BackupRoot;
use crate::net::b2::B2;
use crate::crypto::Key;

mod dirs;
mod files;
pub use files::FileDiff;

/// Use this struct to start diffing folders and to receive `FileDiff`s
pub struct DirDiff<'dirdb> {
    /// Prefixes for list file requests
    list_requests: Vec<(String, &'dirdb DirStat)>,
    receiver: Receiver<FileDiff>,
    sender: Mutex<Cell<Option<Sender<FileDiff>>>>,
    pessimistic_dirdb: Option<DirDB>,
}

impl<'dirdb> DirDiff<'dirdb> {
    pub fn new(local: &DirDB, remote: Option<DirDB>) -> Result<DirDiff, Box<dyn Error>> {
        let (tx_file, rx_file) = channel(16);
        let remote = match remote {
            Some(remote) => remote,
            None => return Ok(DirDiff {
                list_requests: vec![(String::new(), &local.root)],
                receiver: rx_file,
                sender: Mutex::new(Cell::new(Some(tx_file))),
                pessimistic_dirdb: None,
            }),
        };

        let pessimistic_dirdb = DirDB {
            root: dirs::merge_dirstats_pessimistic(&local.root, &remote.root),
        };

        Ok(DirDiff {
            list_requests: vec![(String::new(), &local.root)],
            receiver: rx_file,
            sender: Mutex::new(Cell::new(Some(tx_file))),
            pessimistic_dirdb: Some(pessimistic_dirdb),
        })
    }

    pub fn get_pessimistic_dirdb_data(&self, key: &Key) -> Result<Option<Vec<u8>>, Box<dyn Error>> {
        self.pessimistic_dirdb.as_ref().map(|db| db.to_packed(key)).transpose()
    }

    pub async fn start_diff_remote_files(&'dirdb self, root: &'dirdb BackupRoot, b2: &'dirdb B2) -> Result<(), Box<dyn Error>> {
        let tx_file = self.sender.lock().unwrap().take().expect("Cannot start multiple diffs");
        let futs = self.list_requests.iter().map(|(prefix, stat)| {
            files::diff_files_at(root, b2.clone(), tx_file.clone(), &prefix, stat)
        }).map(|fut| {
            is_ok(fut).boxed()
        });

        let result_vec = await!(futures::future::join_all(futs));
        if let Some(false) = result_vec.iter().find(|&r| !r) {
            Err(From::from("Diff failed".to_string()))
        } else {
            Ok(())
        }
    }
}

impl<'dirdb> Stream for DirDiff<'dirdb> {
    type Item = FileDiff;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.receiver).poll_next(context)
    }
}

/// This works around a rustc bug: https://github.com/rust-lang-nursery/futures-rs/issues/1451
async fn is_ok(f: impl std::future::Future<Output=Result<(), Box<dyn std::error::Error + 'static>>>) -> bool {
    await!(f).is_ok()
}
