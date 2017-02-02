use std::vec::Vec;
use std::error::Error;
use std::iter::Iterator;
use std::path::{Path, PathBuf};
use std::fs;
use std::thread;
use std::sync::mpsc::{channel, sync_channel, Sender, SyncSender, Receiver};
use bincode;
use bincode::rustc_serialize::{encode, decode};
use zstd;
use crypto;
use data::file::{LocalFile, RemoteFile};
use b2api;
use config;

pub struct UploadProgress {
    pub percent: u8,
}

pub struct UploadThread {
    pub tx: SyncSender<LocalFile>,
    pub rx: Receiver<UploadProgress>,
    pub handle: thread::JoinHandle<()>,
}

impl UploadThread {
    fn new(root: &BackupRoot, b2: &b2api::B2) -> UploadThread {
        let root = root.clone();
        let b2: b2api::B2 = b2.to_owned();
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
              rx_file: Receiver<LocalFile>, tx_progress: Sender<UploadProgress>) {
        for file in rx_file {
            // TODO: Check things work. Send progress.
            println!("File to upload: {}", file.rel_path_hash);

            let contents = file.read_all(&root.path);
            if contents.is_err() {
                println!("Failed to read file: {}", file.rel_path_hash);
                // TODO: Send error as progress
                continue;
            }
            let mut contents = contents.unwrap();

            let compressed = zstd::encode_all(contents.as_slice(), config::COMPRESSION_LEVEL);
            contents.clear();
            if compressed.is_err() {
                println!("Failed to compress file: {}", file.rel_path_hash);
                // TODO: Send error as progress
                continue;
            }
            let mut compressed = compressed.unwrap();

            let encrypted = crypto::encrypt(&compressed, &b2.key);
            compressed.clear();

            let filename = root.path_hash.clone()+"/"+&file.rel_path_hash;
            // TODO: Use file's last modification time instead of now
            if b2api::upload_file(&mut b2, &filename, &encrypted, Some(file.last_modified)).is_err() {
                println!("Failed to upload file: {}", file.rel_path_hash);
                // TODO: Send error as progress
                continue;
            }
            // TODO: Send OK as progress
        }
    }
}

#[derive(Clone, RustcEncodable, RustcDecodable, PartialEq)]
pub struct BackupRoot {
    pub path: String,
    pub path_hash: String,
}

impl BackupRoot {
    fn new(path: &String, key: &crypto::Key) -> BackupRoot {
        BackupRoot {
            path: path.clone(),
            path_hash: crypto::hash_path(path, key),
        }
    }

    pub fn list_local_files_async(&self, b2: &b2api::B2)
            -> Result<(Receiver<LocalFile>, thread::JoinHandle<()>), Box<Error>> {
        let (tx, rx) = channel();
        let key = b2.key.clone();
        let path = PathBuf::from(&self.path);
        if !path.is_dir() {
            Err(From::from(format!("{} is not a folder!", &self.path)))
        } else {
            let handle = thread::spawn(move || {
                list_local_files(path.as_path(), path.as_path(), &key.clone(), &tx.clone());
            });
            Ok((rx, handle))
        }
    }

    pub fn list_remote_files(&self, b2: &b2api::B2) -> Result<Vec<RemoteFile>, Box<Error>> {
        let mut files = b2api::list_remote_files(b2, &(self.path_hash.clone()+"/"))?;
        files.sort();
        return Ok(files);
    }

    pub fn start_upload_threads(&self, b2: &b2api::B2) -> Vec<UploadThread> {
        (0..config::UPLOAD_THREADS).map(|_| UploadThread::new(&self, b2)).collect()
    }
}

fn list_local_files(base: &Path, dir: &Path, key: &crypto::Key, tx: &Sender<LocalFile>) {
    for entry in fs::read_dir(dir).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.is_dir() {
            list_local_files(base, &path, &key, tx);
        } else {
            tx.send(LocalFile::new(base, &path, &key).unwrap()).unwrap();
        }
    }
}

pub fn fetch_roots(b2: &b2api::B2) -> Vec<BackupRoot> {
    let mut roots = Vec::new();

    let root_file_data = b2api::download_file(b2, "backup_root");
    if root_file_data.is_ok() {
        roots = decode(&root_file_data.unwrap()[..]).unwrap();
    }

    return roots;
}

pub fn save_roots(b2: &mut b2api::B2, roots: & mut Vec<BackupRoot>) -> Result<(), Box<Error>> {
    let data = encode(roots, bincode::SizeLimit::Infinite)?;
    b2api::upload_file(b2, "backup_root", &data, None)?;
    Ok(())
}

/// Opens an existing backup root, or creates one if necessary
pub fn open_root(b2: &mut b2api::B2, roots: &mut Vec<BackupRoot>, path: &String)
    -> Result<BackupRoot, Box<Error>> {
    {
        let maybe_root = roots.into_iter().find(|r| r.path == *path);
        if maybe_root.is_some() {
            return Ok(maybe_root.unwrap().clone());
        }
    }


    let root = BackupRoot::new(path, &b2.key);
    roots.push(root.clone());
    save_roots(b2, roots)?;

    return Ok(root);
}