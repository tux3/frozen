use std::thread;
use std::sync::mpsc::{channel, sync_channel, Sender, SyncSender, Receiver};
use data::file::{LocalFile};
use data::root::BackupRoot;
use net::b2api;
use crypto;
use progress::{Progress, ProgressDataReader};
use zstd;
use config;

pub struct UploadThread {
    pub tx: SyncSender<Option<LocalFile>>,
    pub rx: Receiver<Progress>,
    pub handle: thread::JoinHandle<()>,
}

impl UploadThread {
    pub fn new(root: &BackupRoot, b2: &b2api::B2) -> UploadThread {
        let root = root.clone();
        let mut b2: b2api::B2 = b2.to_owned();
        b2.upload = None;
        let (tx_file, rx_file) = sync_channel(1);
        let (tx_progress, rx_progress) = channel();
        let handle = thread::spawn(move || {
            UploadThread::upload(root, b2, rx_file, tx_progress)
        });

        UploadThread {
            tx: tx_file,
            rx: rx_progress,
            handle: handle,
        }
    }

    fn upload(root: BackupRoot, mut b2: b2api::B2,
              rx_file: Receiver<Option<LocalFile>>, tx_progress: Sender<Progress>) {
        for file in rx_file {
            if file.is_none() {
                break;
            }
            let file = file.unwrap();

            let filename = file.path_str().into_owned();
            tx_progress.send(Progress::Started(filename.clone())).unwrap();

            let contents = file.read_all(&root.path);
            if contents.is_err() {
                tx_progress.send(Progress::Error(
                                format!("Failed to read file: {}", filename))).unwrap();
                continue;
            }
            let mut contents = contents.unwrap();

            tx_progress.send(Progress::Compressing(0)).unwrap();
            let compressed = zstd::encode_all(contents.as_slice(), config::COMPRESSION_LEVEL);
            contents.clear();
            if compressed.is_err() {
                tx_progress.send(Progress::Error(
                            format!("Failed to compress file: {}", filename))).unwrap();
                continue;
            }
            let mut compressed = compressed.unwrap();

            tx_progress.send(Progress::Encrypting(0)).unwrap();
            let encrypted = crypto::encrypt(&compressed, &b2.key);
            compressed.clear();

            tx_progress.send(Progress::Uploading(0, encrypted.len() as u64)).unwrap();

            let filehash = root.path_hash.clone()+"/"+&file.rel_path_hash;
            let mut progress_reader = ProgressDataReader::new(encrypted, Some(tx_progress.clone()));
            let err = b2api::upload_file(&mut b2, &filehash, &mut progress_reader,
                                         Some(file.last_modified), Some(&filename));
            if err.is_err() {
                tx_progress.send(Progress::Error(
                                format!("Failed to upload file \"{}\": {}", filename,
                                                                err.err().unwrap()))).unwrap();
                continue;
            }
            tx_progress.send(Progress::Transferred(file.path_str().into_owned())).unwrap();
        }

        tx_progress.send(Progress::Terminated).unwrap();
    }
}