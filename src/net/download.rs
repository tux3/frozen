use std::error::Error;
use std::fs::{self, OpenOptions};
use std::os::unix::fs::{symlink, OpenOptionsExt};
use std::io::Write;
use std::path::Path;
use futures::channel::mpsc::{channel, Sender, Receiver};
use futures::{stream::StreamExt, sink::SinkExt};
use crate::data::file::{RemoteFile};
use crate::data::root::BackupRoot;
use crate::net::{b2, progress_thread};
use crate::crypto;
use crate::progress::Progress;
use zstd;

pub struct DownloadThread {
    pub tx: Sender<Option<RemoteFile>>,
    pub rx: Receiver<Progress>,
}

impl progress_thread::ProgressThread for DownloadThread {
    fn progress_rx(&mut self) -> &mut Receiver<Progress> {
        &mut self.rx
    }
}

impl DownloadThread {
    pub fn new(root: &BackupRoot, b2: &b2::B2, target: &str) -> DownloadThread {
        let root = root.clone();
        let target = target.to_owned();
        let (tx_file, rx_file) = channel(1);
        let (tx_progress, rx_progress) = channel(16);
        let mut b2: b2::B2 = b2.to_owned();
        b2.tx_progress = Some(tx_progress.clone());
        crate::futures_compat::tokio_spawn(async {
            let _ = await!(DownloadThread::download(root, b2, target, rx_file, tx_progress));
        });

        DownloadThread {
            tx: tx_file,
            rx: rx_progress,
        }
    }

    async fn download(root: BackupRoot, b2: b2::B2, target: String,
                      mut rx_file: Receiver<Option<RemoteFile>>, mut tx_progress: Sender<Progress>)
                      -> Result<(), Box<dyn Error + 'static>> {
        while let Some(file) = await!(rx_file.next()) {
            if file.is_none() {
                break;
            }
            let file = file.unwrap();

            await!(tx_progress.send(Progress::Started(file.rel_path.clone())))?;

            await!(tx_progress.send(Progress::Downloading(0)))?;
            let filehash = root.path_hash.clone()+"/"+&file.rel_path_hash;
            let encrypted = await!(b2.download_file(&filehash))
                .map_err(|err| Progress::Error(format!("Failed to download file \"{}\": {}", file.rel_path, err)));
            if let Err(err) = encrypted {
                await!(tx_progress.send(err))?;
                continue;
            }
            let mut encrypted = encrypted.unwrap();

            await!(tx_progress.send(Progress::Decrypting(0)))?;
            let compressed = crypto::decrypt(&encrypted, &b2.key)
                .map_err(|err| Progress::Error(format!("Failed to decrypt file \"{}\": {}", file.rel_path, err)));
            encrypted.clear();
            if let Err(err) = compressed {
                await!(tx_progress.send(err))?;
                continue;
            }
            let mut compressed = compressed.unwrap();

            await!(tx_progress.send(Progress::Decompressing(0)))?;
            let contents = zstd::decode_all(compressed.as_slice());
            compressed.clear();
            if contents.is_err() {
                await!(tx_progress.send(Progress::Error(
                    format!("Failed to decompress file \"{}\": {}", file.rel_path,
                            contents.err().unwrap()))))?;
                continue;
            }
            let contents = contents.unwrap();

            let save_path = target.to_owned()+"/"+&file.rel_path;
            if fs::create_dir_all(Path::new(&save_path).parent().unwrap()).is_err() {
                await!(tx_progress.send(Progress::Error(
                    format!("Failed to create path to file \"{}\"", file.rel_path))))?;
                continue;
            }
            fs::remove_file(&save_path).ok();
            if file.is_symlink {
                let link_target = String::from_utf8(contents).unwrap();
                if symlink(link_target, save_path).is_err() {
                    await!(tx_progress.send(Progress::Error(
                        format!("Failed to create symlink \"{}\"", file.rel_path))))?;
                    continue;
                }
            } else {
                let mut options = OpenOptions::new();
                options.mode(file.mode);
                let mut fd = match options.write(true).create(true).truncate(true)
                                    .open(save_path) {
                    Ok(x) => x,
                    Err(_) => {
                        await!(tx_progress.send(Progress::Error(
                            format!("Failed to open file \"{}\"", file.rel_path))))?;
                        continue;
                    },
                };
                if fd.write_all(contents.as_ref()).is_err() {
                    await!(tx_progress.send(Progress::Error(
                        format!("Failed to write file \"{}\"", file.rel_path))))?;
                    continue;
                }
            }

            await!(tx_progress.send(Progress::Transferred(file.rel_path.clone())))?;
        }

        await!(tx_progress.send(Progress::Terminated))?;
        Ok(())
    }
}