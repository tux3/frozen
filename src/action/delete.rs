use std::borrow::Borrow;
use crate::net::rate_limiter::RateLimiter;
use crate::data::root::BackupRoot;
use crate::net::b2::B2;
use crate::data::file::{RemoteFile, RemoteFileVersion};
use crate::progress::ProgressHandler;
use indicatif::ProgressBar;

pub async fn delete(rate_limiter: impl Borrow<RateLimiter>, progress: ProgressHandler,
                    root: impl Borrow<BackupRoot>, b2: impl Borrow<B2>, file: RemoteFile) {
    let _permit_guard = rate_limiter.borrow().borrow_delete_permit().await;
    if progress.verbose() {
        progress.println(format!("Deleting {}", file.rel_path.display()));
    }

    let b2 = b2.borrow();
    let root = root.borrow();

    let version = RemoteFileVersion{
        path: root.path_hash.clone()+"/"+&file.rel_path_hash,
        id: file.id.clone(),
    };

    let err = b2.delete_file_version(&version).await.map_err(|err| {
        format!("Failed to delete last version of \"{}\": {}", file.rel_path.display(), err)
    });
    if let Err(msg) = err {
        progress.report_error(&msg);
        return;
    }

    let path = root.path_hash.clone()+"/"+&file.rel_path_hash;
    let _ = b2.hide_file(&path).await;

    progress.report_success();
}