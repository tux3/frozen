use std::vec::Vec;
use std::error::Error;
use std::iter::Iterator;
use std::path::{Path, PathBuf};
use std::fs;
use std::thread;
use std::sync::mpsc::{channel, Sender, Receiver};
use bincode::{serialize, deserialize, Infinite};
use data_encoding::hex;
use crypto;
use data::file::{LocalFile, RemoteFile, RemoteFileVersion};
use net::b2api;
use net::upload::UploadThread;
use net::download::DownloadThread;
use net::delete::DeleteThread;
use config::Config;
use progress::ProgressDataReader;


#[derive(Clone, Serialize, Deserialize, PartialEq)]
pub struct BackupRoot {
    pub path: String,
    pub path_hash: String,

    #[serde(skip)]
    lock: Option<(RemoteFileVersion, b2api::B2)>,
}

impl BackupRoot {
    fn new(path: &str, key: &crypto::Key) -> BackupRoot {
        BackupRoot {
            path: path.to_owned(),
            path_hash: crypto::hash_path(path, key),
            lock: None,
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
        if self.lock.is_none() {
            return Err(From::from("Cannot list remote files, backup root isn't locked!"));
        }

        let mut files = b2api::list_remote_files(b2, &(self.path_hash.clone()+"/"))?;
        files.sort();
        Ok(files)
    }

    pub fn start_upload_threads(&self, b2: &b2api::B2, config: &Config) -> Vec<UploadThread> {
        (0..config.upload_threads).map(|_| UploadThread::new(self, b2, config)).collect()
    }

    pub fn start_download_threads(&self, b2: &b2api::B2, config: &Config, target: &str) -> Vec<DownloadThread> {
        (0..config.download_threads).map(|_| DownloadThread::new(self, b2, target)).collect()
    }

    pub fn start_delete_threads(&self, b2: &b2api::B2, config: &Config) -> Vec<DeleteThread> {
        (0..config.delete_threads).map(|_| DeleteThread::new(self, b2)).collect()
    }

    pub fn lock(&mut self, b2: &b2api::B2) -> Result<(), Box<Error>> {
        let rand_str = hex::encode(&crypto::randombytes(4));
        let lock_path_prefix = self.path_hash.to_owned()+"/lock.";
        let lock_path = lock_path_prefix.to_owned()+&rand_str;
        let mut lock_b2 = b2.clone();

        let mut data_reader = ProgressDataReader::new(Vec::new(), None);
        let lock_version = b2api::upload_file(&mut lock_b2, &lock_path, &mut data_reader, None)?;
        let locks = b2api::list_remote_file_versions(&lock_b2, &lock_path_prefix);
        self.lock = Some((lock_version, lock_b2));

        if locks.is_err() {
            let _ = self.unlock();
            return Err(locks.err().unwrap());
        }
        let locks = locks.unwrap();

        if locks.len() > 1 {
            let _ = self.unlock();

            return Err(From::from(format!("Failed to lock the backup root, {} lock already exists",
                                            locks.len() - 1)));
        }

        println!("Locked root folder");
        Ok(())
    }

    pub fn unlock(&mut self) -> Result<(), Box<Error>> {
        if self.lock.is_none() {
            return Ok(());
        }
        let (lock_version, lock_b2) = self.lock.take().unwrap();
        b2api::delete_file_version(&lock_b2, &lock_version)?;
        println!("Unlocked root folder");
        Ok(())
    }
}

impl Drop for BackupRoot {
    fn drop(&mut self) {
        let _ = self.unlock();
    }
}

fn list_local_files(base: &Path, dir: &Path, key: &crypto::Key, tx: &Sender<LocalFile>) {
    let entries = fs::read_dir(dir);
    if entries.is_err() {
        println!("Couldn't open folder \"{}\": {}", (base.to_string_lossy()+dir.to_string_lossy()),
                                                        entries.err().unwrap());
        return
    }
    for entry in entries.unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        let is_symlink = entry.file_type().and_then(|ft| Ok(ft.is_symlink())).unwrap_or(false);
        if path.is_dir() && !is_symlink {
            list_local_files(base, &path, key, tx);
        } else {
            let file = LocalFile::new(base, &path, key);
            tx.send(file.unwrap()).unwrap();
        }
    }
}

pub fn fetch_roots(b2: &b2api::B2) -> Vec<BackupRoot> {
    let mut roots = Vec::new();

    let root_file_data = b2api::download_file(b2, "backup_root");
    if root_file_data.is_ok() {
        roots = deserialize(&root_file_data.unwrap()[..]).unwrap();
    }

    roots
}

pub fn save_roots(b2: &mut b2api::B2, roots: & mut Vec<BackupRoot>) -> Result<(), Box<Error>> {
    let data = serialize(roots, Infinite)?;
    let mut data_reader = ProgressDataReader::new(data, None);
    b2api::upload_file(b2, "backup_root", &mut data_reader, None)?;
    Ok(())
}

/// Opens an existing backup root, or creates one if necessary
pub fn open_create_root(b2: &mut b2api::B2, roots: &mut Vec<BackupRoot>, path: &str)
    -> Result<BackupRoot, Box<Error>> {
    {
        let maybe_root = roots.into_iter().find(|r| r.path == *path);
        if maybe_root.is_some() {
            let mut root = maybe_root.unwrap().clone();
            root.lock(b2)?;
            return Ok(root);
        }
    }


    let mut root = BackupRoot::new(path, &b2.key);
    roots.push(root.clone());
    save_roots(b2, roots)?;

    root.lock(b2)?;
    Ok(root)
}

pub fn delete_root(b2: &mut b2api::B2, roots: &mut Vec<BackupRoot>, path: &str)
    -> Result<(), Box<Error>> {
    if roots.iter().position(|r| r.path == path).map(|i| roots.remove(i)).is_none() {
        return Err(From::from(format!("Backup does not exist for \"{}\", nothing to delete", path)))
    }

    save_roots(b2, roots)?;

    Ok(())
}

/// Opens an existing backup root
pub fn open_root(b2: &b2api::B2, roots: &mut Vec<BackupRoot>, path: &str)
                        -> Result<BackupRoot, Box<Error>> {
    match roots.into_iter().find(|r| r.path == *path) {
        Some(root) => {
            let mut root = root.clone();
            root.lock(b2)?;
            Ok(root)
        },
        None => Err(From::from(format!("Backup does not exist for \"{}\"", path))),
    }
}

/// Forcibly unlocks a backup root
pub fn wipe_locks(b2: &mut b2api::B2, roots: &Vec<BackupRoot>, path: &str)
                        -> Result<(), Box<Error>> {
    if let Some(root) = roots.into_iter().find(|r| r.path == *path) {
        let lock_path_prefix = root.path_hash.to_owned() + "/lock.";
        let locks = b2api::list_remote_file_versions(&b2, &lock_path_prefix)?;

        println!("{} lock files to remove", locks.len());
        for lock_version in &locks {
            b2api::delete_file_version(&b2, &lock_version)?;
        }
        Ok(())
    } else {
        Err(From::from(format!("Backup does not exist for \"{}\"", path)))
    }
}