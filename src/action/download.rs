use std::borrow::Borrow;
use std::path::{PathBuf, Path};
use std::io::Write;
use std::fs::{self, OpenOptions};
use std::os::unix::fs::{symlink, OpenOptionsExt};
use crate::net::rate_limiter::RateLimiter;
use crate::data::file::RemoteFile;
use crate::data::root::BackupRoot;
use crate::net::b2::B2;
use crate::termio::progress::{progress_output, Progress};
use crate::crypto;

pub async fn download(rate_limiter: impl Borrow<RateLimiter>, root: impl Borrow<BackupRoot>, b2: impl Borrow<B2>,
                      target_path: impl Borrow<PathBuf>, file: RemoteFile) {
    let b2 = b2.borrow();
    let root = root.borrow();

    let mut permit = rate_limiter.borrow().borrow_download_permit().await;
    progress_output(Progress::Started(file.rel_path.display().to_string()));
    progress_output(Progress::Downloading(0));

    let filehash = root.path_hash.clone()+"/"+&file.rel_path_hash;
    let encrypted = b2.download_file(&filehash).await
        .map_err(|err| Progress::Error(format!("Failed to download file \"{}\": {}", file.rel_path.display(), err)));
    if let Err(err) = encrypted {
        progress_output(err);
        return;
    }
    let mut encrypted = encrypted.unwrap();

    progress_output(Progress::Decrypting(0));
    let compressed = crypto::decrypt(&encrypted, &b2.key)
        .map_err(|err| Progress::Error(format!("Failed to decrypt file \"{}\": {}", file.rel_path.display(), err)));
    encrypted.clear();
    if let Err(err) = compressed {
        progress_output(err);
        return;
    }
    let mut compressed = compressed.unwrap();

    progress_output(Progress::Decompressing(0));
    let contents = zstd::decode_all(compressed.as_slice());
    compressed.clear();
    if contents.is_err() {
        progress_output(Progress::Error(
            format!("Failed to decompress file \"{}\": {}", file.rel_path.display(),
                    contents.err().unwrap())));
        return;
    }
    let contents = contents.unwrap();

    if save_file(&file, contents, target_path.borrow()).await.is_ok() {
        progress_output(Progress::Transferred(file.rel_path.display().to_string()));
    }
}

async fn save_file(file: &RemoteFile, contents: Vec<u8>, target: &Path) -> Result<(), ()> {
    let save_path = target.join(&file.rel_path);
    if fs::create_dir_all(Path::new(&save_path).parent().unwrap()).is_err() {
        progress_output(Progress::Error(format!("Failed to create path to file \"{}\"", file.rel_path.display())));
        return Err(());
    }
    fs::remove_file(&save_path).ok();
    if file.is_symlink {
        let link_target = String::from_utf8(contents).unwrap();
        if symlink(link_target, save_path).is_err() {
            progress_output(Progress::Error(format!("Failed to create symlink \"{}\"", file.rel_path.display())));
            return Err(());
        }
    } else {
        let mut options = OpenOptions::new();
        options.mode(file.mode);
        let mut fd = match options.write(true).create(true).truncate(true)
            .open(save_path) {
            Ok(x) => x,
            Err(_) => {
                progress_output(Progress::Error(format!("Failed to open file \"{}\"", file.rel_path.display())));
                return Err(());
            },
        };
        if fd.write_all(contents.as_ref()).is_err() {
            progress_output(Progress::Error(format!("Failed to write file \"{}\"", file.rel_path.display())));
            return Err(());
        }
    }
    Ok(())
}