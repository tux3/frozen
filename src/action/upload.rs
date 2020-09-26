use crate::crypto;
use crate::data::file::LocalFile;
use crate::net::rate_limiter::RateLimiter;
use crate::progress::ProgressHandler;
use crate::stream::{CompressionStream, EncryptionStream};
use eyre::WrapErr;
use std::borrow::Borrow;
use std::io::Cursor;
use std::path::PathBuf;

pub async fn upload(
    rate_limiter: impl Borrow<RateLimiter>,
    progress: ProgressHandler,
    compression_level: i32,
    root_path: impl Borrow<PathBuf>,
    file: LocalFile,
) {
    let root_path = root_path.borrow();
    let rel_path = &file.rel_path;

    let rate_limiter = rate_limiter.borrow();
    let mut permit = rate_limiter.borrow_upload_permit().await;
    let b2 = rate_limiter.b2_client();

    if progress.verbose() {
        progress.println(format!("Uploading {}", file.rel_path.display()));
    }

    if permit.is_none() {
        let upload_url = match b2.get_upload_url().await {
            Ok(upload_url) => upload_url,
            Err(err) => {
                progress.report_error(&format!(
                    "Failed to start upload for file \"{}\": {}",
                    rel_path.display(),
                    err
                ));
                return;
            }
        };
        *permit = Some(upload_url);
    }
    let upload_url = permit.as_ref().unwrap();

    let is_symlink = file.is_symlink_at(root_path).unwrap_or(false);
    let compressed_stream = if is_symlink {
        let link_data = file.readlink_at(root_path).ok();
        match link_data {
            Some(data) => Some(CompressionStream::new(Cursor::new(data), compression_level).await),
            None => None,
        }
    } else {
        let path = file.full_path(root_path);
        let std_file = std::fs::File::open(path).ok();
        match std_file {
            Some(file) => Some(CompressionStream::new(file, compression_level).await),
            None => None,
        }
    };
    let compressed_stream = match compressed_stream {
        Some(c) => Box::new(c),
        None => {
            progress.report_error(format!("Failed to read file: {}", rel_path.display()));
            return;
        }
    };

    let encrypted_stream = EncryptionStream::new(compressed_stream, &b2.key);

    let filehash = &file.full_path_hash;
    let enc_meta = crypto::encode_meta(&b2.key, &rel_path, file.last_modified, file.mode, is_symlink);

    let err = b2
        .upload_file_stream(upload_url, filehash, encrypted_stream, Some(enc_meta))
        .await
        .wrap_err_with(|| format!("Failed to upload file \"{}\"", rel_path.display()));
    if let Err(err) = err {
        progress.report_error(format!("{:#}", err));
        permit.take(); // The upload_url might be invalid now, let's get a new one
        return;
    }
    progress.report_success();
}
