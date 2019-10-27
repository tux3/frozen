use std::borrow::Borrow;
use crate::net::rate_limiter::RateLimiter;
use crate::data::root::BackupRoot;
use crate::net::b2::B2;
use crate::data::file::{RemoteFile, RemoteFileVersion};
use crate::termio::progress::{Progress, progress_output};

pub async fn delete(rate_limiter: impl Borrow<RateLimiter>, root: impl Borrow<BackupRoot>, b2: impl Borrow<B2>, file: RemoteFile) {
    let permit = rate_limiter.borrow().borrow_download_permit().await;

    progress_output(Progress::Started(file.rel_path.display().to_string()));
    progress_output(Progress::Deleting);

    let b2 = b2.borrow();
    let root = root.borrow();

    let version = RemoteFileVersion{
        path: root.path_hash.clone()+"/"+&file.rel_path_hash,
        id: file.id.clone(),
    };

    let err = b2.delete_file_version(&version).await.map_err(|err| {
        Progress::Error(format!("Failed to delete last version of \"{}\": {}", file.rel_path.display(), err))
    });
    if let Err(Progress::Error(msg)) = err {
        progress_output(Progress::Error(msg));
        return;
    }

    let path = root.path_hash.clone()+"/"+&file.rel_path_hash;
    let _ = b2.hide_file(&path).await;

    progress_output(Progress::Deleted(file.rel_path.display().to_string()));
}