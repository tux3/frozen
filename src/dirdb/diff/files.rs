use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use std::pin::Pin;
use futures::{Stream, Poll, SinkExt};
use futures::task::Context;
use futures::channel::mpsc::Sender;
use super::{FileStat, DirStat};
use crate::crypto;
use crate::data::root::BackupRoot;
use crate::data::file::{RemoteFile, LocalFile};
use crate::net::b2::B2;

pub struct FileDiff {
    pub local: Option<LocalFile>,
    pub remote: Option<RemoteFile>,
}

pub struct FileDiffStream {
}

impl FileDiffStream {
    pub fn new() -> Self {
        Self {

        }
    }
}

impl Stream for FileDiffStream {
    type Item = FileDiff;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        unimplemented!()
    }
}

fn flatten_dirstat_files(files: &mut HashMap<String, LocalFile>, stat: &DirStat, key: &crypto::Key) {
    for file in stat.direct_files.as_ref().unwrap() {
        let lfile = LocalFile{
            rel_path: file.rel_path.clone(),
            rel_path_hash: crypto::hash_path(&file.rel_path, key),
            last_modified: file.last_modified,
            mode: file.mode
        };
        files.insert(lfile.rel_path_hash.clone(), lfile);
    }
    for dir in stat.subfolders.iter() {
        flatten_dirstat_files(files, dir, key);
    }
}

pub async fn diff_files_at<'dirdb>(root: &BackupRoot, b2: &B2, mut tx: Sender<FileDiff>,
                               prefix: &'dirdb str, stat: &'dirdb DirStat) -> Result<(), Box<dyn Error + 'static>> {
    let remote_files = root.list_remote_files_at(&b2, &prefix).await?;
    println!("Listed {} remote files! Local folder has {} files.", remote_files.len(), stat.total_files_count);

    let mut local_files = HashMap::new();
    flatten_dirstat_files(&mut local_files, stat, &b2.key);
    println!("Local files map has {} entries", local_files.len());

    for rfile in remote_files.into_iter() {
        if let Some(lfile) =  local_files.remove(&rfile.rel_path_hash) {
            if lfile.last_modified != rfile.last_modified {
                tx.send(FileDiff{
                    local: Some(lfile),
                    remote: Some(rfile),
                }).await?;
            }
        } else {
            tx.send(FileDiff{
                local: None,
                remote: Some(rfile),
            }).await?;
        }
    }
    for (_, lfile) in local_files.into_iter() {
        tx.send(FileDiff{
            local: Some(lfile),
            remote: None,
        }).await?;
    }

    Ok(())
}
