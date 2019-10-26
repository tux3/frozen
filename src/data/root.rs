use std::vec::Vec;
use std::error::Error;
use std::iter::Iterator;
use std::path::{Path, PathBuf};
use std::fs;
use std::thread;
use std::sync::mpsc::{channel, Sender, Receiver};
use bincode::{serialize, deserialize};
use data_encoding::{HEXLOWER_PERMISSIVE};
use serde::{Serialize, Deserialize};
use crate::crypto;
use crate::data::file::{LocalFile, RemoteFile, RemoteFileVersion};
use crate::net::b2;
use crate::net::download::DownloadThread;
use crate::net::delete::DeleteThread;
use crate::config::Config;
use crate::termio::progress::ProgressDataReader;

#[derive(Clone, Serialize, Deserialize)]
pub struct BackupRoot {
    pub path: PathBuf,
    pub path_hash: String,

    #[serde(skip)]
    lock: Option<(RemoteFileVersion, b2::B2)>,
}

impl BackupRoot {
    fn new(path: &Path, key: &crypto::Key) -> BackupRoot {
        BackupRoot {
            path: path.to_owned(),
            path_hash: crypto::hash_path(path, key),
            lock: None,
        }
    }

    pub fn rename(&mut self, new_path: PathBuf) {
        self.path = new_path;
    }

    pub fn list_local_files_async(&self, b2: &b2::B2, path: &Path)
                                     -> Result<(Receiver<LocalFile>, thread::JoinHandle<()>), Box<dyn Error>> {
        let (tx, rx) = channel();
        let key = b2.key.clone();
        if !path.is_dir() {
            Err(From::from(format!("{} is not a folder!", &self.path.display())))
        } else {
            let path = path.to_owned();
            let handle = thread::spawn(move || {
                let _ = list_local_files(&path, &path, &key.clone(), &tx.clone());
            });
            Ok((rx, handle))
        }
    }

    pub async fn list_remote_files<'a>(&'a self, b2: &'a b2::B2) -> Result<Vec<RemoteFile>, Box<dyn Error + 'static>> {
        self.list_remote_files_at(b2, "").await
    }

    pub async fn list_remote_files_at<'a>(&'a self, b2: &'a b2::B2, prefix: &'a str) -> Result<Vec<RemoteFile>, Box<dyn Error + 'static>> {
        if self.lock.is_none() {
            return Err(From::from("Cannot list remote files, backup root isn't locked!"));
        }

        let path = self.path_hash.clone()+"/"+prefix;
        let mut files = b2.list_remote_files(&path).await?;
        files.sort();
        Ok(files)
    }

    pub fn start_download_threads(&self, b2: &b2::B2, config: &Config, target: &Path) -> Vec<DownloadThread> {
        (0..config.download_threads).map(|_| DownloadThread::new(self, b2, target)).collect()
    }

    pub fn start_delete_threads(&self, b2: &b2::B2, config: &Config) -> Vec<DeleteThread> {
        (0..config.delete_threads).map(|_| DeleteThread::new(self, b2)).collect()
    }

    pub async fn lock<'a>(&'a mut self, b2: &'a b2::B2) -> Result<(), Box<dyn Error + 'static>> {
        let rand_str = HEXLOWER_PERMISSIVE.encode(&crypto::randombytes(4));
        let lock_path_prefix = self.path_hash.to_owned()+".lock.";
        let lock_path = lock_path_prefix.to_owned()+&rand_str;

        let lock_version = b2.upload_file_simple(&lock_path, Vec::new()).await?;
        let locks = b2.list_remote_file_versions(&lock_path_prefix).await;
        self.lock = Some((lock_version, b2.clone()));

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

        Ok(())
    }

    pub async fn unlock(&mut self) -> Result<(), Box<dyn Error + 'static>> {
        if self.lock.is_none() {
            return Ok(());
        }
        let (version, b2) = self.lock.take().unwrap();
        b2.delete_file_version(&version).await
    }
}

fn list_local_files(base: &Path, dir: &Path, key: &crypto::Key, tx: &Sender<LocalFile>)
    -> Result<(), Box<dyn Error>> {
    let entries = fs::read_dir(dir);
    if entries.is_err() {
        println!("Couldn't open folder \"{}\": {}", base.join(dir).display(),
                                                        entries.err().unwrap());
        return Ok(())
    }
    for entry in entries.unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        let is_symlink = entry.file_type().and_then(|ft| Ok(ft.is_symlink())).unwrap_or(false);
        if path.is_dir() && !is_symlink {
            list_local_files(base, &path, key, tx)?;
        } else {
            let file = LocalFile::new(base, &path, key);
            if tx.send(file.unwrap()).is_err() {
                return Err(From::from("Main thread seems to be gone, exiting"));
            }
        }
    }
    Ok(())
}

pub async fn fetch_roots(b2: &b2::B2) -> Result<Vec<BackupRoot>, Box<dyn Error + 'static>> {
    let enc_data = match b2.download_file("backup_root").await {
        Ok(enc_data) => enc_data,
        Err(_) => return Ok(Vec::new()),
    };
    let data = crypto::decrypt(&enc_data, &b2.key)?;
    Ok(deserialize(&data[..]).unwrap())
}

pub async fn save_roots<'a>(b2: &'a b2::B2, roots: &'a[BackupRoot]) -> Result<(), Box<dyn Error + 'static>> {
    let plain_data = serialize(roots)?;
    let data = crypto::encrypt(&plain_data, &b2.key);
    b2.upload_file_simple("backup_root", data).await?;
    Ok(())
}

/// Opens an existing backup root, or creates one if necessary
pub async fn open_create_root<'a>(b2: &'a b2::B2, roots: &'a mut Vec<BackupRoot>, path: &'a Path)
                                  -> Result<BackupRoot, Box<dyn Error + 'static>> {
    let mut root: BackupRoot;
    if let Some(existing_root) = roots.iter_mut().find(|r| r.path == *path) {
        root = existing_root.clone();
    } else {
        root = BackupRoot::new(path, &b2.key);
        roots.push(root.clone());
        save_roots(b2, roots).await?;
    }

    root.lock(b2).await?;
    Ok(root)
}

pub async fn delete_root<'a>(b2: &'a mut b2::B2, roots: &'a mut Vec<BackupRoot>, path: &'a Path)
                             -> Result<(), Box<dyn Error + 'static>> {
    if roots.iter()
        .position(|r| r.path == path)
        .map(|i| roots.remove(i))
        .is_none() {
        Err(From::from(format!("Backup does not exist for \"{}\", nothing to delete", path.display())))
    } else {
        save_roots(b2, roots).await
    }
}

/// Opens an existing backup root
pub async fn open_root<'a>(b2: &'a b2::B2, roots: &'a mut Vec<BackupRoot>, path: &'a Path)
                           -> Result<BackupRoot, Box<dyn Error + 'static>> {
    match roots.iter().find(|r| r.path == path) {
        Some(root) => {
            let mut root = root.clone();
            root.lock(b2).await?;
            Ok(root)
        },
        None => Err(From::from(format!("Backup does not exist for \"{}\"", path.display()))),
    }
}

/// Forcibly unlocks a backup root
pub async fn wipe_locks<'a>(b2: &'a mut b2::B2, roots: &'a[BackupRoot], path: &'a Path)
                            -> Result<(), Box<dyn Error + 'static>> {
    if let Some(root) = roots.iter().find(|r| r.path == *path) {
        let lock_path_prefix = root.path_hash.to_owned() + ".lock.";
        let locks = b2.list_remote_file_versions(&lock_path_prefix).await?;

        println!("{} lock files to remove", locks.len());
        for lock_version in &locks {
            b2.delete_file_version(&lock_version).await?;
        }
        Ok(())
    } else {
        Err(From::from(format!("Backup does not exist for \"{}\"", path.display())))
    }
}
