use self::files::FileDiffStream;
use super::{DirDB, DirStat};
use crate::crypto::Key;
use crate::data::root::BackupRoot;
use crate::net::b2::B2;
use eyre::Result;
use futures::stream::{SelectAll, Stream, StreamExt};
use futures::task::Poll;
use owning_ref::ArcRef;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;

mod dirs;
mod files;
pub use files::FileDiff;

/// Use this struct to start diffing folders and to receive `FileDiff`s
pub struct DirDiff {
    diff_stream: SelectAll<FileDiffStream>,
    pessimistic_dirdb: DirDB,
}

impl DirDiff {
    pub fn new(root: Arc<BackupRoot>, b2: Arc<B2>, local: Arc<DirDB>, remote: &Option<DirDB>) -> Result<DirDiff> {
        let empty_remote = DirDB::new_empty();
        let remote = remote.as_ref().unwrap_or(&empty_remote);
        let pessimistic_dirdb = DirDB {
            root: dirs::merge_dirstats_pessimistic(&local.root, &remote.root),
        };

        let local = ArcRef::new(local).map(|db| &db.root);
        let diff_stream = dirs::diff_dirs(root, b2, local, &remote.root);

        Ok(DirDiff {
            diff_stream,
            pessimistic_dirdb,
        })
    }

    pub fn get_pessimistic_dirdb_data(&self, key: &Key) -> Result<Vec<u8>> {
        self.pessimistic_dirdb.to_packed(key)
    }
}

impl Stream for DirDiff {
    type Item = Result<FileDiff>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context) -> Poll<Option<Self::Item>> {
        self.diff_stream.poll_next_unpin(context)
    }
}
