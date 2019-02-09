use std::error::Error;
use std::path::{PathBuf, Path};
use std::borrow::Borrow;
use zstd;
use futures::channel::mpsc::{channel, Sender, Receiver};
use futures::{sink::SinkExt, stream::StreamExt};
use crate::data::file::{LocalFile};
use crate::data::root::BackupRoot;
use crate::net::{b2, progress_thread};
use crate::config::Config;
use crate::crypto;
use crate::termio::progress::{Progress, ProgressDataReader, progress_output};

pub struct UploadThread {
    pub tx: Sender<Option<LocalFile>>,
    pub rx: Receiver<Progress>,
}

impl progress_thread::ProgressThread for UploadThread {
    fn progress_rx(&mut self) -> &mut Receiver<Progress> {
        &mut self.rx
    }
}

impl UploadThread {
    pub fn new(root: &BackupRoot, b2: &b2::B2, config: &Config, source_path: &Path) -> UploadThread {
        let root = root.clone();
        let source_path = source_path.to_owned();
        let config = config.clone();
        let (tx_file, rx_file) = channel(1);
        let (tx_progress, rx_progress) = channel(16);
        let mut b2 = b2.to_owned();
        b2.tx_progress = Some(tx_progress.clone());

        tokio::spawn(async {
            let _ = await!(UploadThread::upload(root, b2, config, source_path, rx_file, tx_progress));
        });

        UploadThread {
            tx: tx_file,
            rx: rx_progress,
        }
    }

    async fn upload(root: BackupRoot, b2: b2::B2, config: Config, source_path: PathBuf,
                    mut rx_file: Receiver<Option<LocalFile>>, mut tx_progress: Sender<Progress>)
                    -> Result<(), Box<dyn Error + 'static>> {
        while let Some(file) = await!(rx_file.next()) {
            if file.is_none() {
                break;
            }
            let file = file.unwrap();

            upload(&root, &b2, config.compression_level, source_path.clone(), file).await;
        }

        await!(tx_progress.send(Progress::Terminated))?;
        Ok(())
    }
}

pub async fn upload(root: impl Borrow<BackupRoot>, b2: impl Borrow<b2::B2>,
                    compression_level: i32, source_path: impl Borrow<PathBuf>, file: LocalFile) {
    let filename = &file.rel_path;
    let source_path = source_path.borrow();
    let b2 = b2.borrow();
    let root = root.borrow();
    progress_output(Progress::Started(filename.display().to_string()));

    let is_symlink = file.is_symlink(source_path).unwrap_or(false);
    let mut contents = {
        let maybe_contents = if is_symlink {
            file.readlink(source_path)
        } else {
            file.read_all(source_path)
        }.map_err(|_| Progress::Error(format!("Failed to read file: {}", filename.display())));

        match maybe_contents {
            Ok(contents) => contents,
            Err(err) => {
                progress_output(err);
                return;
            }
        }
    };

    progress_output(Progress::Compressing(0));
    let compressed = zstd::block::compress(contents.as_slice(), compression_level);
    contents.clear();
    contents.shrink_to_fit();
    if compressed.is_err() {
        progress_output(Progress::Error(format!("Failed to compress file: {}", filename.display())));
        return;
    }
    let mut compressed = compressed.unwrap();

    progress_output(Progress::Encrypting(0));
    let encrypted = crypto::encrypt(&compressed, &b2.key);
    compressed.clear();
    compressed.shrink_to_fit();

    progress_output(Progress::Uploading(0, encrypted.len() as u64));

    let filehash = root.path_hash.clone()+"/"+&file.rel_path_hash;
    let progress_reader = ProgressDataReader::new(encrypted);
    let enc_meta = crypto::encode_meta(&b2.key, &filename, file.last_modified,
                                       file.mode, is_symlink);

    let err = await!(b2.upload_file(&filehash, progress_reader, Some(enc_meta))).map_err(|err| {
        Progress::Error(format!("Failed to upload file \"{}\": {}", filename.display(), err))
    });
    if let Err(err) = err {
        progress_output(err);
        return;
    }
    progress_output(Progress::Transferred(file.rel_path.display().to_string()));
}