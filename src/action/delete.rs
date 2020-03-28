use crate::data::file::{RemoteFile, RemoteFileVersion};
use crate::net::rate_limiter::RateLimiter;
use crate::progress::ProgressHandler;
use std::borrow::Borrow;

pub async fn delete(rate_limiter: impl Borrow<RateLimiter>, progress: ProgressHandler, file: RemoteFile) {
    let rate_limiter = rate_limiter.borrow();
    let _permit_guard = rate_limiter.borrow_delete_permit().await;
    if progress.verbose() {
        progress.println(format!("Deleting {}", file.rel_path.display()));
    }

    let b2 = rate_limiter.b2_client();

    let version = RemoteFileVersion {
        path: file.full_path_hash.clone(),
        id: file.id.clone(),
    };

    let err = b2.delete_file_version(&version).await.map_err(|err| {
        format!(
            "Failed to delete last version of \"{}\": {}",
            file.rel_path.display(),
            err
        )
    });
    if let Err(msg) = err {
        progress.report_error(&msg);
        return;
    }

    let _ = b2.hide_file(&file.full_path_hash).await;

    progress.report_success();
}
