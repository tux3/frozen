use std::thread;
use std::sync::mpsc::{channel, sync_channel, Sender, SyncSender, Receiver};
use data::file::{RemoteFile};
use data::root::BackupRoot;
use net::b2api;
use crypto;
use progress::Progress;
use zstd;

pub struct DownloadThread {
    pub tx: SyncSender<Option<RemoteFile>>,
    pub rx: Receiver<Progress>,
    pub handle: thread::JoinHandle<()>,
}

impl DownloadThread {
    pub fn new(root: &BackupRoot, b2: &b2api::B2, target: &str) -> DownloadThread {
        let root = root.clone();
        let b2: b2api::B2 = b2.to_owned();
        let target = target.to_owned();
        let (tx_file, rx_file) = sync_channel(1);
        let (tx_progress, rx_progress) = channel();
        let handle = thread::spawn(move || {
            DownloadThread::download(root, b2, target, rx_file, tx_progress)
        });

        DownloadThread {
            tx: tx_file,
            rx: rx_progress,
            handle: handle,
        }
    }

    fn download(root: BackupRoot, b2: b2api::B2, target: String,
                rx_file: Receiver<Option<RemoteFile>>, tx_progress: Sender<Progress>) {
        for file in rx_file {
            if file.is_none() {
                break;
            }
            let file = file.unwrap();

            // TODO: Download and unpack
            tx_progress.send(Progress::Started(file.rel_path.clone())).unwrap();

            tx_progress.send(Progress::Downloading(0)).unwrap();
            let filehash = root.path_hash.clone()+"/"+&file.rel_path_hash;
            let encrypted = b2api::download_file(&b2, &filehash);
            if encrypted.is_err() {
                println!("Failed to download file: {}", file.rel_path);
                // TODO: Send error as progress
                continue;
            }
            let mut encrypted = encrypted.unwrap();

            tx_progress.send(Progress::Decrypting(0)).unwrap();
            let compressed = crypto::decrypt(&encrypted, &b2.key);
            encrypted.clear();
            if compressed.is_err() {
                println!("Failed to decrypt file: {}", file.rel_path);
                // TODO: Send error as progress
                continue;
            }
            let mut compressed = compressed.unwrap();

            tx_progress.send(Progress::Decompressing(0)).unwrap();
            let contents = zstd::decode_all(compressed.as_slice());
            compressed.clear();
            if contents.is_err() {
                println!("Failed to decompress file: {}", file.rel_path);
                // TODO: Send error as progress
                continue;
            }

            // TODO: Extract filename and save file

            tx_progress.send(Progress::Transferred(file.rel_path.clone())).unwrap();
        }

        tx_progress.send(Progress::Terminated).unwrap();
    }
}