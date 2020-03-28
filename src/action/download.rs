use crate::data::file::RemoteFile;
use crate::net::rate_limiter::RateLimiter;
use crate::progress::ProgressHandler;
use crate::stream::{DecompressionStream, DecryptionStream};
use futures::StreamExt;
use std::borrow::Borrow;
use std::fs::{self, OpenOptions};
use std::os::unix::fs::{symlink, OpenOptionsExt};
use std::path::{Path, PathBuf};

pub async fn download(
    rate_limiter: impl Borrow<RateLimiter>,
    progress: ProgressHandler,
    target_path: impl Borrow<PathBuf>,
    file: RemoteFile,
) {
    let rate_limiter = rate_limiter.borrow();
    let mut _permit_guard = rate_limiter.borrow_download_permit().await;
    let b2 = rate_limiter.b2_client();

    if progress.verbose() {
        progress.println(format!("Downloading {}", file.rel_path.display()));
    }

    let encrypted = b2
        .download_file_stream(&file.full_path_hash)
        .await
        .map_err(|err| format!("Failed to download file \"{}\": {}", file.rel_path.display(), err));
    let encrypted = match encrypted {
        Err(err) => {
            progress.report_error(&err);
            return;
        }
        Ok(data) => data,
    };

    let decrypted_stream = DecryptionStream::new(encrypted, &b2.key);

    if save_file(&file, decrypted_stream, target_path.borrow(), &progress)
        .await
        .is_ok()
    {
        progress.report_success();
    }
}

async fn save_file(
    file: &RemoteFile,
    mut decrypted_stream: DecryptionStream,
    target: &Path,
    progress: &ProgressHandler,
) -> Result<(), ()> {
    let save_path = target.join(&file.rel_path);
    if fs::create_dir_all(Path::new(&save_path).parent().unwrap()).is_err() {
        progress.report_error(&format!(
            "Failed to create path to file \"{}\"",
            file.rel_path.display()
        ));
        return Err(());
    }
    let _ = fs::remove_file(&save_path);
    if file.is_symlink {
        let mut compressed_buf = Vec::<u8>::new();
        while let Some(compressed) = decrypted_stream.next().await {
            match compressed {
                Err(err) => {
                    progress.report_error(&format!("Failed to decrypt \"{}\": {}", file.rel_path.display(), err));
                    return Err(());
                }
                Ok(compressed) => compressed_buf.extend_from_slice(&compressed),
            }
        }
        let decompressed = match zstd::decode_all(compressed_buf.as_slice()) {
            Err(err) => {
                progress.report_error(&format!(
                    "Failed to decompress \"{}\": {}",
                    file.rel_path.display(),
                    err
                ));
                return Err(());
            }
            Ok(data) => data,
        };

        let link_target = String::from_utf8(decompressed).unwrap();
        if symlink(link_target, save_path).is_err() {
            progress.report_error(&format!("Failed to create symlink \"{}\"", file.rel_path.display()));
            return Err(());
        }
    } else {
        let mut options = OpenOptions::new();
        options.mode(file.mode);
        let fd = match options.write(true).create(true).truncate(true).open(save_path) {
            Ok(x) => x,
            Err(_) => {
                progress.report_error(&format!("Failed to open file \"{}\"", file.rel_path.display()));
                return Err(());
            }
        };
        let mut decompressed_stream = DecompressionStream::new(Box::new(decrypted_stream), fd);
        while let Some(result) = decompressed_stream.next().await {
            if let Err(err) = result {
                progress.report_error(&format!(
                    "Failed to decrypt/decompress \"{}\": {}",
                    file.rel_path.display(),
                    err
                ));
            }
        }
    }
    Ok(())
}
