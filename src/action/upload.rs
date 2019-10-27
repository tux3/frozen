use std::borrow::Borrow;
use std::path::PathBuf;
use crate::crypto;
use crate::data::file::LocalFile;
use crate::data::root::BackupRoot;
use crate::net::b2::B2;
use crate::termio::progress::{progress_output, Progress, ProgressDataReader};
use crate::net::rate_limiter::RateLimiter;
use std::ops::{Deref, DerefMut};

pub async fn upload(rate_limiter: impl Borrow<RateLimiter>, root: impl Borrow<BackupRoot>, b2: impl Borrow<B2>,
                    compression_level: i32, root_path: impl Borrow<PathBuf>, file: LocalFile) {
    let root_path = root_path.borrow();
    let filename = &file.rel_path;
    let b2 = b2.borrow();
    let root = root.borrow();

    let mut permit = rate_limiter.borrow().borrow_upload_permit().await;

    progress_output(Progress::Started(filename.display().to_string()));
    if permit.is_none() {
        let upload_url = match b2.get_upload_url().await {
            Ok(upload_url) => upload_url,
            Err(err) => {
                progress_output(Progress::Error(format!("Failed to start upload for file \"{}\": {}", filename.display(), err)));
                return;
            }
        };
        *permit = Some(upload_url);
    }
    let upload_url = permit.as_ref().unwrap();

    let is_symlink = file.is_symlink_at(root_path).unwrap_or(false);
    let mut contents = {
        let maybe_contents = if is_symlink {
            file.readlink_at(root_path)
        } else {
            file.read_all_at(root_path)
        }.map_err(|_| Progress::Error(format!("Failed to read file: {}", filename.display())));

        match maybe_contents {
            Ok(contents) => contents,
            Err(err) => {
                progress_output(err);
                return;
            }
        }
    };

    progress_output(Progress::Compressing(0));
    let compressed = zstd::block::compress(contents.as_slice(), compression_level);
    contents.clear();
    contents.shrink_to_fit();
    if compressed.is_err() {
        progress_output(Progress::Error(format!("Failed to compress file: {}", filename.display())));
        return;
    }
    let mut compressed = compressed.unwrap();

    progress_output(Progress::Encrypting(0));
    let encrypted = crypto::encrypt(&compressed, &b2.key);
    compressed.clear();
    compressed.shrink_to_fit();

    progress_output(Progress::Uploading(0, encrypted.len() as u64));

    let filehash = root.path_hash.clone()+"/"+&file.rel_path_hash;
    let progress_reader = ProgressDataReader::new(encrypted);
    let enc_meta = crypto::encode_meta(&b2.key, &filename, file.last_modified,
                                       file.mode, is_symlink);

    let err = b2.upload_file(upload_url, &filehash, progress_reader, Some(enc_meta)).await.map_err(|err| {
        Progress::Error(format!("Failed to upload file \"{}\": {}", filename.display(), err))
    });
    if let Err(err) = err {
        progress_output(err);
        permit.take(); // The upload_url might be invalid now, let's get a new one
        return;
    }
    progress_output(Progress::Transferred(file.rel_path.display().to_string()));
}