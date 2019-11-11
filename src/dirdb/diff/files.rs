use super::{DirDB, DirStat};
use crate::box_result::BoxResult;
use crate::crypto;
use crate::data::file::{LocalFile, RemoteFile};
use crate::data::paths::filename_to_bytes;
use crate::data::root::BackupRoot;
use crate::net::b2::B2;
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
        list_fut: LocalBoxFuture<'static, BoxResult<Vec<RemoteFile>>>,
    },
    DiffFiles {
        diff_stream: LocalBoxStream<'static, BoxResult<FileDiff>>,
    },
    Failed,
}

pub struct FileDiffStream {
    state: FileDiffStreamState,
    b2: Arc<B2>,
    dir_stat: ArcRef<DirDB, DirStat>,
    dir_path_hash: Option<String>,
}

impl FileDiffStream {
    pub fn new(root: Arc<BackupRoot>, b2: Arc<B2>, prefix: String, dir_stat: ArcRef<DirDB, DirStat>) -> Self {
        let dir_path_hash = root.path_hash.clone() + &prefix;

        let b2_clone = b2.clone();
        let list_fut = async move { root.list_remote_files_at(&b2_clone, &prefix).await }.boxed_local();

        Self {
            state: FileDiffStreamState::DownloadFileList { list_fut },
            b2,
            dir_stat,
            dir_path_hash: Some(dir_path_hash),
        }
    }

    fn make_diff_stream(
        local_files: HashMap<String, LocalFile>,
        remote_files: Vec<RemoteFile>,
    ) -> impl Stream<Item = BoxResult<FileDiff>> {
        enum LocalFilesEnum<F: FnMut((String, LocalFile)) -> FileDiff> {
            HashMap(HashMap<String, LocalFile>),
            RemainingIter(std::iter::Map<IntoIter<String, LocalFile>, F>),
        };
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

    fn flatten_dirstat_files(
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
        list_fut_poll: Poll<BoxResult<Vec<RemoteFile>>>,
    ) -> Poll<Option<BoxResult<FileDiff>>> {
        match list_fut_poll {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(e)) => {
                self.state = FileDiffStreamState::Failed;
                Poll::Ready(Some(Err(e)))
            }
            Poll::Ready(Ok(remote_files)) => {
                let mut local_files = HashMap::new();
                let mut dir_path_hash = self.dir_path_hash.take().unwrap();
                Self::flatten_dirstat_files(&mut local_files, &self.dir_stat, &mut dir_path_hash, &self.b2.key);

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
    type Item = BoxResult<FileDiff>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.state {
            FileDiffStreamState::DownloadFileList { ref mut list_fut } => {
                let list_fut_poll = list_fut.poll_unpin(cx);
                self.poll_download_fut(cx, list_fut_poll)
            }
            FileDiffStreamState::DiffFiles { ref mut diff_stream } => diff_stream.poll_next_unpin(cx),
            FileDiffStreamState::Failed => Poll::Ready(None),
        }
    }
}
