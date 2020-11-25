use super::{DirDB, DirStat};
use crate::crypto;
use crate::data::file::{LocalFile, RemoteFile};
use crate::data::paths::filename_to_bytes;
use crate::data::root::BackupRoot;
use crate::net::b2::{FileListDepth, B2};
use eyre::Result;
use futures::future::{FutureExt, LocalBoxFuture};
use futures::stream::{LocalBoxStream, Stream, StreamExt};
use futures::task::{Context, Poll};
use hashbrown::hash_map::{HashMap, IntoIter};
use owning_ref::ArcRef;
use std::pin::Pin;
use std::sync::Arc;

pub struct FileDiff {
    pub local: Option<LocalFile>,
    pub remote: Option<RemoteFile>,
}

enum FileDiffStreamState {
    DownloadFileList {
        list_fut: LocalBoxFuture<'static, Result<Vec<RemoteFile>>>,
        key: crypto::Key,
        depth: FileListDepth,
    },
    DiffFiles {
        diff_stream: LocalBoxStream<'static, Result<FileDiff>>,
    },
    Failed,
}

pub struct FileDiffStream {
    state: FileDiffStreamState,
    dir_stat: Option<ArcRef<DirDB, DirStat>>,
    dir_path_hash: Option<String>,
}

impl FileDiffStream {
    /// Creates a stream that will list and diff remote files
    pub fn new(
        root: Arc<BackupRoot>,
        b2: Arc<B2>,
        prefix: String,
        dir_stat: Option<ArcRef<DirDB, DirStat>>,
        deep_diff: bool,
    ) -> Self {
        let dir_path_hash = root.path_hash.clone() + &prefix;

        let depth = if deep_diff {
            FileListDepth::Deep
        } else {
            FileListDepth::Shallow
        };
        let b2_clone = b2.clone();
        let list_fut = async move { root.list_remote_files_at(&b2_clone, &prefix, depth).await }.boxed_local();

        Self {
            state: FileDiffStreamState::DownloadFileList {
                list_fut,
                key: b2.key.clone(),
                depth,
            },
            dir_stat,
            dir_path_hash: Some(dir_path_hash),
        }
    }

    /// Creates a stream that returns the files in a local directory not present on the remote
    pub fn new_local(
        root: Arc<BackupRoot>,
        prefix: String,
        dir_stat: ArcRef<DirDB, DirStat>,
        key: &crypto::Key,
    ) -> Self {
        let mut local_files = HashMap::new();
        let mut dir_path_hash = root.path_hash.clone() + &prefix;
        Self::flatten_dirstat_files(&mut local_files, &dir_stat, &mut dir_path_hash, &key);

        let diff_iter = local_files.into_iter().map(|(_, lfile)| {
            Ok(FileDiff {
                local: Some(lfile),
                remote: None,
            })
        });
        let diff_stream = futures::stream::iter(diff_iter).boxed_local();

        Self {
            state: FileDiffStreamState::DiffFiles { diff_stream },
            dir_stat: None,
            dir_path_hash: None,
        }
    }

    fn make_diff_stream(
        local_files: HashMap<String, LocalFile>,
        remote_files: Vec<RemoteFile>,
    ) -> impl Stream<Item = Result<FileDiff>> {
        enum LocalFilesEnum<F: FnMut((String, LocalFile)) -> FileDiff> {
            HashMap(HashMap<String, LocalFile>),
            RemainingIter(std::iter::Map<IntoIter<String, LocalFile>, F>),
        }
        let mut local_files_enum = LocalFilesEnum::HashMap(local_files);
        let mut remote_files_iter = remote_files.into_iter();

        let diff_next = move || match local_files_enum {
            LocalFilesEnum::HashMap(ref mut local_files) => {
                #[allow(clippy::while_let_on_iterator)]
                // This is a FnMut, we can't consume the iterator!
                while let Some(rfile) = remote_files_iter.next() {
                    if let Some(lfile) = local_files.remove(&rfile.full_path_hash) {
                        if lfile.last_modified != rfile.last_modified {
                            return Some(FileDiff {
                                local: Some(lfile),
                                remote: Some(rfile),
                            });
                        }
                    } else {
                        return Some(FileDiff {
                            local: None,
                            remote: Some(rfile),
                        });
                    }
                }

                let local_files = std::mem::replace(local_files, HashMap::new());
                let mut iter = local_files.into_iter().map(|(_, lfile)| FileDiff {
                    local: Some(lfile),
                    remote: None,
                });
                let next = iter.next();
                local_files_enum = LocalFilesEnum::RemainingIter(iter);
                next
            }
            LocalFilesEnum::RemainingIter(ref mut local_files_iter) => local_files_iter.next(),
        };
        futures::stream::iter(std::iter::from_fn(diff_next).map(Result::Ok))
    }

    fn flatten_dirstat_files_shallow(
        files: &mut HashMap<String, LocalFile>,
        dirstat: &DirStat,
        dir_path_hash: &mut String,
        key: &crypto::Key,
    ) {
        for filestat in dirstat.direct_files.as_ref().unwrap() {
            let mut full_path_hash = dir_path_hash.clone();
            crypto::hash_path_filename_into(
                dir_path_hash.as_bytes(),
                filename_to_bytes(&filestat.rel_path).unwrap(),
                key,
                &mut full_path_hash,
            );

            let lfile = LocalFile {
                rel_path: filestat.rel_path.clone(),
                full_path_hash,
                last_modified: filestat.last_modified,
                mode: filestat.mode,
            };
            files.insert(lfile.full_path_hash.clone(), lfile);
        }
    }

    fn flatten_dirstat_files(
        files: &mut HashMap<String, LocalFile>,
        dirstat: &DirStat,
        dir_path_hash: &mut String,
        key: &crypto::Key,
    ) {
        Self::flatten_dirstat_files_shallow(files, dirstat, dir_path_hash, key);

        let cur_dir_path_hash_len = dir_path_hash.len();
        for subdir in dirstat.subfolders.iter() {
            dir_path_hash.truncate(cur_dir_path_hash_len);
            base64::encode_config_buf(&subdir.dir_name_hash, base64::URL_SAFE_NO_PAD, dir_path_hash);
            dir_path_hash.push('/');
            Self::flatten_dirstat_files(files, subdir, dir_path_hash, key);
        }
    }

    fn poll_download_fut(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        list_fut_poll: Poll<Result<Vec<RemoteFile>>>,
        key: crypto::Key,
        depth: FileListDepth,
    ) -> Poll<Option<Result<FileDiff>>> {
        match list_fut_poll {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(e)) => {
                self.state = FileDiffStreamState::Failed;
                Poll::Ready(Some(Err(e)))
            }
            Poll::Ready(Ok(remote_files)) => {
                let mut local_files = HashMap::new();

                // For remote-only diffs the local dir stat is None
                if let Some(ref local_dir_stat) = self.dir_stat.take() {
                    let mut dir_path_hash = self.dir_path_hash.take().unwrap();
                    if let FileListDepth::Deep = depth {
                        Self::flatten_dirstat_files(&mut local_files, local_dir_stat, &mut dir_path_hash, &key);
                    } else {
                        Self::flatten_dirstat_files_shallow(&mut local_files, local_dir_stat, &mut dir_path_hash, &key);
                    }
                }

                let mut diff_stream = Self::make_diff_stream(local_files, remote_files);
                let next = diff_stream.poll_next_unpin(cx);

                self.state = FileDiffStreamState::DiffFiles {
                    diff_stream: diff_stream.boxed_local(),
                };

                next
            }
        }
    }
}

impl Stream for FileDiffStream {
    type Item = Result<FileDiff>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match &mut self.state {
            FileDiffStreamState::DownloadFileList { list_fut, key, depth } => {
                let list_fut_poll = list_fut.poll_unpin(cx);
                let key = key.clone();
                let depth = *depth;
                self.poll_download_fut(cx, list_fut_poll, key, depth)
            }
            FileDiffStreamState::DiffFiles { diff_stream } => diff_stream.poll_next_unpin(cx),
            FileDiffStreamState::Failed => Poll::Ready(None),
        }
    }
}

#[cfg(test)]
mod test {
    use crate::dirdb::diff::files::FileDiffStream;
    use crate::test_helpers::*;
    use futures::{executor::block_on, StreamExt};
    use owning_ref::ArcRef;
    use std::sync::Arc;

    #[test]
    fn local_diff_stream_returns_all_files() {
        let key = test_key();
        let prefix = "/".to_string();
        let root = Arc::new(test_backup_root(&key));
        let dirdb = ArcRef::new(Arc::new(test_dirdb()));
        let dirstat = dirdb.map(|d| &d.root);

        let mut stream = FileDiffStream::new_local(root, prefix, dirstat, &key);

        let mut filenames = vec![];
        while let Some(item) = block_on(stream.next()) {
            let item = item.unwrap();
            assert!(!item.remote.is_some()); // This is a local stream
            let local_file = item.local.unwrap();
            filenames.push(local_file.rel_path.to_str().unwrap().to_string());
        }
        filenames.sort();

        // Yup. We're gleefuly hardcoding the contents for this test!
        assert_eq!(filenames, vec!["a", "b", "dir/c"]);
    }
}
