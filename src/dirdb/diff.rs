use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use futures::Poll;
use futures::stream::{Stream, StreamExt, SelectAll};
use owning_ref::ArcRef;
use self::files::FileDiffStream;
use super::{DirDB, DirStat};
use crate::box_result::BoxResult;
use crate::data::root::BackupRoot;
use crate::net::b2::B2;
use crate::crypto::Key;

mod dirs;
mod files;
pub use files::FileDiff;

/// Use this struct to start diffing folders and to receive `FileDiff`s
pub struct DirDiff {
    diff_stream: SelectAll<FileDiffStream>,
    pessimistic_dirdb: DirDB,
}

impl DirDiff {
    pub fn new(root: Arc<BackupRoot>, b2: Arc<B2>, local: Arc<DirDB>, remote: Option<DirDB>) -> BoxResult<DirDiff> {
        let mut diff_stream = SelectAll::new();
        let local: ArcRef<DirDB> = local.into();
        diff_stream.push(FileDiffStream::new(root, b2, "/".to_owned(), local.clone().map(|db| &db.root)));

        let remote = match remote {
            Some(remote) => remote,
            None => DirDB::new_empty(),
        };

        let pessimistic_dirdb = DirDB {
            root: dirs::merge_dirstats_pessimistic(&local.root, &remote.root),
        };

        Ok(DirDiff {
            diff_stream,
            pessimistic_dirdb,
        })
    }

    pub fn get_pessimistic_dirdb_data(&self, key: &Key) -> BoxResult<Vec<u8>> {
        self.pessimistic_dirdb.to_packed(key)
    }
}

impl Stream for DirDiff {
    type Item = BoxResult<FileDiff>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context) -> Poll<Option<Self::Item>> {
        self.diff_stream.poll_next_unpin(context)
    }
}
