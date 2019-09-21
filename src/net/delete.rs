use std::error::Error;
use futures::channel::mpsc::{channel, Sender, Receiver};
use futures::{stream::StreamExt, sink::SinkExt};
use crate::data::file::{RemoteFile, RemoteFileVersion};
use crate::data::root::BackupRoot;
use crate::net::{b2, progress_thread};
use crate::termio::progress::{Progress, progress_output};
use std::borrow::Borrow;

pub struct DeleteThread {
    pub tx: Sender<Option<RemoteFile>>,
    pub rx: Receiver<Progress>,
}

impl progress_thread::ProgressThread for DeleteThread {
    fn progress_rx(&mut self) -> &mut Receiver<Progress> {
        &mut self.rx
    }
}

impl DeleteThread {
    pub fn new(root: &BackupRoot, b2: &b2::B2) -> DeleteThread {
        let root = root.clone();
        let (tx_file, rx_file) = channel(1);
        let (tx_progress, rx_progress) = channel(16);
        let mut b2 = b2.to_owned();
        b2.tx_progress = Some(tx_progress.clone());

        tokio::spawn(async {
            let _ = DeleteThread::delete(root, b2, rx_file, tx_progress).await;
        });

        DeleteThread {
            tx: tx_file,
            rx: rx_progress,
        }
    }

    async fn delete(root: BackupRoot, b2: b2::B2,
                    mut rx_file: Receiver<Option<RemoteFile>>, mut tx_progress: Sender<Progress>)
                    -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        while let Some(file) = rx_file.next().await {
            if file.is_none() {
                break;
            }
            let file = file.unwrap();

            delete(&root, &b2, file).await;
        }

        tx_progress.send(Progress::Terminated).await?;
        Ok(())
    }
}

pub async fn delete(root: impl Borrow<BackupRoot>, b2: impl Borrow<b2::B2>, file: RemoteFile) {
    progress_output(Progress::Started(file.rel_path.display().to_string()));
    progress_output(Progress::Deleting);

    let b2 = b2.borrow();
    let root = root.borrow();

    let version = RemoteFileVersion{
        path: root.path_hash.clone()+"/"+&file.rel_path_hash,
        id: file.id.clone(),
    };

    let err = b2.delete_file_version(&version).await.map_err(|err| {
        Progress::Error(format!("Failed to delete last version of \"{}\": {}", file.rel_path.display(), err))
    });
    if let Err(Progress::Error(msg)) = err {
        progress_output(Progress::Error(msg));
        return;
    }

    let path = root.path_hash.clone()+"/"+&file.rel_path_hash;
    let _ = b2.hide_file(&path).await;

    progress_output(Progress::Deleted(file.rel_path.display().to_string()));
}