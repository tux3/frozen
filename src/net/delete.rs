use std::error::Error;
use futures::channel::mpsc::{channel, Sender, Receiver};
use futures::{stream::StreamExt, sink::SinkExt};
use crate::data::file::{RemoteFile, RemoteFileVersion};
use crate::data::root::BackupRoot;
use crate::net::{b2, progress_thread};
use crate::progress::Progress;

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

        crate::futures_compat::tokio_spawn(async {
            let _ = await!(DeleteThread::delete(root, b2, rx_file, tx_progress));
        });

        DeleteThread {
            tx: tx_file,
            rx: rx_progress,
        }
    }

    async fn delete(root: BackupRoot, b2: b2::B2,
                    mut rx_file: Receiver<Option<RemoteFile>>, mut tx_progress: Sender<Progress>)
                    -> Result<(), Box<dyn Error + 'static>> {
        while let Some(file) = await!(rx_file.next()) {
            if file.is_none() {
                break;
            }
            let file = file.unwrap();

            await!(tx_progress.send(Progress::Started(file.rel_path.clone())))?;
            await!(tx_progress.send(Progress::Deleting))?;

            let version = RemoteFileVersion{
                path: root.path_hash.clone()+"/"+&file.rel_path_hash,
                id: file.id.clone(),
            };

            let err = await!(b2.delete_file_version(&version)).map_err(|err| {
                Progress::Error(format!("Failed to delete last version of \"{}\": {}", file.rel_path, err))
            });
            if let Err(err) = err {
                await!(tx_progress.send(err))?;
                continue;
            }

            let path = root.path_hash.clone()+"/"+&file.rel_path_hash;
            let _ = await!(b2.hide_file(&path));

            await!(tx_progress.send(Progress::Deleted(file.rel_path.clone())))?;
        }

        await!(tx_progress.send(Progress::Terminated))?;
        Ok(())
    }
}