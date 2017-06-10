use std::thread;
use std::error::Error;
use std::sync::mpsc::{channel, sync_channel, Sender, SyncSender, Receiver};
use data::file::{RemoteFile, RemoteFileVersion};
use data::root::BackupRoot;
use net::{b2api, progress_thread};
use progress::Progress;

pub struct DeleteThread {
    pub tx: SyncSender<Option<RemoteFile>>,
    pub rx: Receiver<Progress>,
    pub handle: thread::JoinHandle<()>,
}

impl progress_thread::ProgressThread for DeleteThread {
    fn progress_rx(&self) -> &Receiver<Progress> {
        &self.rx
    }
}

impl DeleteThread {
    pub fn new(root: &BackupRoot, b2: &b2api::B2) -> DeleteThread {
        let root = root.clone();
        let b2: b2api::B2 = b2.to_owned();
        let (tx_file, rx_file) = sync_channel(1);
        let (tx_progress, rx_progress) = channel();
        let handle = thread::spawn(move || {
            let _ = DeleteThread::delete(root, b2, rx_file, tx_progress);
        });

        DeleteThread {
            tx: tx_file,
            rx: rx_progress,
            handle: handle,
        }
    }

    fn delete(root: BackupRoot, b2: b2api::B2,
                rx_file: Receiver<Option<RemoteFile>>, tx_progress: Sender<Progress>)
            -> Result<(), Box<Error>> {
        for file in rx_file {
            if file.is_none() {
                break;
            }
            let file = file.unwrap();

            tx_progress.send(Progress::Started(file.rel_path.clone()))?;
            tx_progress.send(Progress::Deleting)?;

            let version = RemoteFileVersion{
                path: root.path_hash.clone()+"/"+&file.rel_path_hash,
                id: file.id,
            };
            let err = b2api::delete_file_version(&b2, &version);
            if err.is_err() {
                tx_progress.send(Progress::Error(
                    format!("Failed to delete last version of \"{}\": {}", file.rel_path,
                            err.err().unwrap())))?;
                continue;
            }

            let _ = b2api::hide_file(&b2, &(root.path_hash.clone()+"/"+&file.rel_path_hash));

            tx_progress.send(Progress::Deleted(file.rel_path.clone()))?;
        }

        tx_progress.send(Progress::Terminated)?;
        Ok(())
    }
}