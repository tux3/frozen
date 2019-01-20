use std::thread;
use std::error::Error;
use std::fs::{self, OpenOptions};
use std::os::unix::fs::{symlink, OpenOptionsExt};
use std::io::Write;
use std::path::Path;
use std::sync::mpsc::{channel, sync_channel, Sender, SyncSender, Receiver};
use data::file::{RemoteFile};
use data::root::BackupRoot;
use net::{b2api, progress_thread};
use crypto;
use progress::Progress;
use zstd;

pub struct DownloadThread {
    pub tx: SyncSender<Option<RemoteFile>>,
    pub rx: Receiver<Progress>,
    pub handle: thread::JoinHandle<()>,
}

impl progress_thread::ProgressThread for DownloadThread {
    fn progress_rx(&self) -> &Receiver<Progress> {
        &self.rx
    }
}

impl DownloadThread {
    pub fn new(root: &BackupRoot, b2: &b2api::B2, target: &str) -> DownloadThread {
        let root = root.clone();
        let b2: b2api::B2 = b2.to_owned();
        let target = target.to_owned();
        let (tx_file, rx_file) = sync_channel(1);
        let (tx_progress, rx_progress) = channel();
        let handle = thread::spawn(move || {
            let _ = DownloadThread::download(root, b2, target, rx_file, tx_progress);
        });

        DownloadThread {
            tx: tx_file,
            rx: rx_progress,
            handle: handle,
        }
    }

    fn download(root: BackupRoot, b2: b2api::B2, target: String,
                rx_file: Receiver<Option<RemoteFile>>, tx_progress: Sender<Progress>)
            -> Result<(), Box<Error>> {
        for file in rx_file {
            if file.is_none() {
                break;
            }
            let file = file.unwrap();

            tx_progress.send(Progress::Started(file.rel_path.clone()))?;

            tx_progress.send(Progress::Downloading(0))?;
            let filehash = root.path_hash.clone()+"/"+&file.rel_path_hash;
            let encrypted = b2api::download_file(&b2, &filehash);
            if encrypted.is_err() {
                tx_progress.send(Progress::Error(
                    format!("Failed to download file \"{}\": {}", file.rel_path,
                            encrypted.err().unwrap())))?;
                continue;
            }
            let mut encrypted = encrypted.unwrap();

            tx_progress.send(Progress::Decrypting(0))?;
            let compressed = crypto::decrypt(&encrypted, &b2.key);
            encrypted.clear();
            if compressed.is_err() {
                tx_progress.send(Progress::Error(
                    format!("Failed to decrypt file \"{}\": {}", file.rel_path,
                            compressed.err().unwrap())))?;
                continue;
            }
            let mut compressed = compressed.unwrap();

            tx_progress.send(Progress::Decompressing(0))?;
            let contents = zstd::decode_all(compressed.as_slice());
            compressed.clear();
            if contents.is_err() {
                tx_progress.send(Progress::Error(
                    format!("Failed to decompress file \"{}\": {}", file.rel_path,
                            contents.err().unwrap())))?;
                continue;
            }
            let contents = contents.unwrap();

            let save_path = target.to_owned()+"/"+&file.rel_path;
            if fs::create_dir_all(Path::new(&save_path).parent().unwrap()).is_err() {
                tx_progress.send(Progress::Error(
                    format!("Failed to create path to file \"{}\"", file.rel_path)))?;
                continue;
            }

            fs::remove_file(&save_path).ok();
            if file.is_symlink {
                let link_target = String::from_utf8(contents).unwrap();
                if symlink(link_target, save_path).is_err() {
                    tx_progress.send(Progress::Error(
                        format!("Failed to create symlink \"{}\"", file.rel_path)))?;
                    continue;
                }
            } else {
                let mut options = OpenOptions::new();
                options.mode(file.mode);
                let mut fd = match options.write(true).create(true).truncate(true)
                                    .open(save_path) {
                    Ok(x) => x,
                    Err(_) => {
                        tx_progress.send(Progress::Error(
                            format!("Failed to open file \"{}\"", file.rel_path)))?;
                        continue;
                    },
                };
                if fd.write_all(contents.as_ref()).is_err() {
                    tx_progress.send(Progress::Error(
                        format!("Failed to write file \"{}\"", file.rel_path)))?;
                    continue;
                }
            }

            tx_progress.send(Progress::Transferred(file.rel_path.clone()))?;
        }

        tx_progress.send(Progress::Terminated)?;
        Ok(())
    }
}