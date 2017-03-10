use std::vec::Vec;
use std::error::Error;
use std::iter::Iterator;
use std::path::{Path, PathBuf};
use std::fs;
use std::thread;
use std::sync::mpsc::{channel, Sender, Receiver};
use bincode;
use bincode::rustc_serialize::{encode, decode};
use crypto;
use data::file::{LocalFile, RemoteFile};
use net::b2api;
use net::upload::UploadThread;
use net::download::DownloadThread;
use config;
use progress::ProgressDataReader;

#[derive(Clone, RustcEncodable, RustcDecodable, PartialEq)]
pub struct BackupRoot {
    pub path: String,
    pub path_hash: String,
}

impl BackupRoot {
    fn new(path: &str, key: &crypto::Key) -> BackupRoot {
        BackupRoot {
            path: path.to_owned(),
            path_hash: crypto::hash_path(path, key),
        }
    }

    pub fn list_local_files_async(&self, b2: &b2api::B2)
            -> Result<(Receiver<LocalFile>, thread::JoinHandle<()>), Box<Error>> {
        self.list_local_files_async_at(b2, &self.path)
    }

    pub fn list_local_files_async_at(&self, b2: &b2api::B2, path: &str)
                                  -> Result<(Receiver<LocalFile>, thread::JoinHandle<()>), Box<Error>> {
        let (tx, rx) = channel();
        let key = b2.key.clone();
        let path = PathBuf::from(path);
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
        Ok(files)
    }

    pub fn start_upload_threads(&self, b2: &b2api::B2) -> Vec<UploadThread> {
        (0..config::UPLOAD_THREADS).map(|_| UploadThread::new(self, b2)).collect()
    }

    pub fn start_download_threads(&self, b2: &b2api::B2, target: &str) -> Vec<DownloadThread> {
        (0..config::DOWNLOAD_THREADS).map(|_| DownloadThread::new(self, b2, target)).collect()
    }
}

fn list_local_files(base: &Path, dir: &Path, key: &crypto::Key, tx: &Sender<LocalFile>) {
    for entry in fs::read_dir(dir).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.is_dir() {
            list_local_files(base, &path, key, tx);
        } else {
            tx.send(LocalFile::new(base, &path, key).unwrap()).unwrap();
        }
    }
}

pub fn fetch_roots(b2: &b2api::B2) -> Vec<BackupRoot> {
    let mut roots = Vec::new();

    let root_file_data = b2api::download_file(b2, "backup_root");
    if root_file_data.is_ok() {
        roots = decode(&root_file_data.unwrap()[..]).unwrap();
    }

    roots
}

pub fn save_roots(b2: &mut b2api::B2, roots: & mut Vec<BackupRoot>) -> Result<(), Box<Error>> {
    let data = encode(roots, bincode::SizeLimit::Infinite)?;
    let mut data_reader = ProgressDataReader::new(data, None);
    b2api::upload_file(b2, "backup_root", &mut data_reader, None, None)?;
    Ok(())
}

/// Opens an existing backup root, or creates one if necessary
pub fn open_create_root(b2: &mut b2api::B2, roots: &mut Vec<BackupRoot>, path: &str)
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

    Ok(root)
}

/// Opens an existing backup root
pub fn open_root(roots: &mut Vec<BackupRoot>, path: &str)
                        -> Result<BackupRoot, Box<Error>> {
    match roots.into_iter().find(|r| r.path == *path) {
        Some(root) => Ok(root.clone()),
        None => Err(From::from(format!("Backup does not exist for \"{}\"", path))),
    }
}