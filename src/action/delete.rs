use std::borrow::Borrow;
use crate::net::rate_limiter::RateLimiter;
use crate::net::b2::B2;
use crate::data::file::{RemoteFile, RemoteFileVersion};
use crate::progress::ProgressHandler;

pub async fn delete(rate_limiter: impl Borrow<RateLimiter>, progress: ProgressHandler,
                    b2: impl Borrow<B2>, file: RemoteFile) {
    let _permit_guard = rate_limiter.borrow().borrow_delete_permit().await;
    if progress.verbose() {
        progress.println(format!("Deleting {}", file.rel_path.display()));
    }

    let b2 = b2.borrow();

    let version = RemoteFileVersion {
        path: file.full_path_hash.clone(),
        id: file.id.clone(),
    };

    let err = b2.delete_file_version(&version).await.map_err(|err| {
        format!("Failed to delete last version of \"{}\": {}", file.rel_path.display(), err)
    });
    if let Err(msg) = err {
        progress.report_error(&msg);
        return;
    }

    let _ = b2.hide_file(&file.full_path_hash).await;

    progress.report_success();
}