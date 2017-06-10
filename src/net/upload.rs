use std::thread;
use std::error::Error;
use std::sync::mpsc::{channel, sync_channel, Sender, SyncSender, Receiver};
use data::file::{LocalFile};
use data::root::BackupRoot;
use net::{b2api, progress_thread};
use config::Config;
use crypto;
use progress::{Progress, ProgressDataReader};
use zstd;

pub struct UploadThread {
    pub tx: SyncSender<Option<LocalFile>>,
    pub rx: Receiver<Progress>,
    pub handle: thread::JoinHandle<()>,
}

impl progress_thread::ProgressThread for UploadThread {
    fn progress_rx(&self) -> &Receiver<Progress> {
        &self.rx
    }
}

impl UploadThread {
    pub fn new(root: &BackupRoot, b2: &b2api::B2, config: &Config) -> UploadThread {
        let root = root.clone();
        let config = config.clone();
        let mut b2: b2api::B2 = b2.to_owned();
        b2.upload = None;
        let (tx_file, rx_file) = sync_channel(1);
        let (tx_progress, rx_progress) = channel();
        let handle = thread::spawn(move || {
            let _ = UploadThread::upload(root, b2, config, rx_file, tx_progress);
        });

        UploadThread {
            tx: tx_file,
            rx: rx_progress,
            handle: handle,
        }
    }

    fn upload(root: BackupRoot, mut b2: b2api::B2, config: Config,
              rx_file: Receiver<Option<LocalFile>>, tx_progress: Sender<Progress>)
                -> Result<(), Box<Error>> {
        for file in rx_file {
            if file.is_none() {
                break;
            }
            let file = file.unwrap();

            let filename = file.path_str().into_owned();
            tx_progress.send(Progress::Started(filename.clone()))?;

            let is_symlink = file.is_symlink(&root.path).unwrap_or(false);
            let contents = if is_symlink {
                file.readlink(&root.path)
            } else {
                file.read_all(&root.path)
            };

            if contents.is_err() {
                tx_progress.send(Progress::Error(format!("Failed to read file: {}", filename)))?;
                continue;
            }
            let mut contents = contents.unwrap();

            tx_progress.send(Progress::Compressing(0))?;
            let compressed = zstd::block::compress(contents.as_slice(), config.compression_level);
            contents.clear();
            contents.shrink_to_fit();
            if compressed.is_err() {
                tx_progress.send(Progress::Error(
                                    format!("Failed to compress file: {}", filename)))?;
                continue;
            }
            let mut compressed = compressed.unwrap();

            tx_progress.send(Progress::Encrypting(0))?;
            let encrypted = crypto::encrypt(&compressed, &b2.key);
            compressed.clear();
            compressed.shrink_to_fit();

            tx_progress.send(Progress::Uploading(0, encrypted.len() as u64))?;

            let filehash = root.path_hash.clone()+"/"+&file.rel_path_hash;
            let mut progress_reader = ProgressDataReader::new(encrypted, Some(tx_progress.clone()));
            let enc_meta = crypto::encode_meta(&b2.key, &filename, file.last_modified, is_symlink);
            let err = b2api::upload_file(&mut b2, &filehash, &mut progress_reader, Some(enc_meta));
            if err.is_err() {
                tx_progress.send(Progress::Error(
                                format!("Failed to upload file \"{}\": {}", filename,
                                                                err.err().unwrap())))?;
                continue;
            }
            tx_progress.send(Progress::Transferred(file.path_str().into_owned()))?;
        }

        tx_progress.send(Progress::Terminated)?;
        Ok(())
    }
}