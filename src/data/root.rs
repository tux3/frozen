use std::vec::Vec;
use std::iter::Iterator;
use std::path::{Path, PathBuf};
use bincode::{serialize, deserialize};
use data_encoding::{HEXLOWER_PERMISSIVE};
use serde::{Serialize, Deserialize};
use crate::crypto;
use crate::data::file::{RemoteFile, RemoteFileVersion};
use crate::net::b2;
use crate::box_result::BoxResult;

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
            path_hash: crypto::hash_path_root(path, key),
            lock: None,
        }
    }

    pub fn rename(&mut self, new_path: PathBuf) {
        self.path = new_path;
    }

    pub async fn list_remote_files<'a>(&'a self, b2: &'a b2::B2) -> BoxResult<Vec<RemoteFile>> {
        self.list_remote_files_at(b2, "/").await
    }

    pub async fn list_remote_files_at<'a>(&'a self, b2: &'a b2::B2, prefix: &'a str) -> BoxResult<Vec<RemoteFile>> {
        if self.lock.is_none() {
            return Err(From::from("Cannot list remote files, backup root isn't locked!"));
        }

        // We assume the prefix is a relative path hash, starting and ending with /
        debug_assert!(prefix.chars().next() == Some('/'));
        debug_assert!(prefix.chars().last() == Some('/'));

        let path = self.path_hash.clone()+prefix;
        let mut files = b2.list_remote_files(&path).await?;
        files.sort();
        Ok(files)
    }

    pub async fn lock<'a>(&'a mut self, b2: &'a b2::B2) -> BoxResult<()> {
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

    pub async fn unlock(&mut self) -> BoxResult<()> {
        if self.lock.is_none() {
            return Ok(());
        }
        let (version, b2) = self.lock.take().unwrap();
        b2.delete_file_version(&version).await
    }
}

pub async fn fetch_roots(b2: &b2::B2) -> BoxResult<Vec<BackupRoot>> {
    let enc_data = match b2.download_file("backup_root").await {
        Ok(enc_data) => enc_data,
        Err(_) => return Ok(Vec::new()),
    };
    let data = crypto::decrypt(&enc_data, &b2.key)?;
    Ok(deserialize(&data[..]).unwrap())
}

pub async fn save_roots<'a>(b2: &'a b2::B2, roots: &'a[BackupRoot]) -> BoxResult<()> {
    let plain_data = serialize(roots)?;
    let data = crypto::encrypt(&plain_data, &b2.key);
    b2.upload_file_simple("backup_root", data).await?;
    Ok(())
}

/// Opens an existing backup root, or creates one if necessary
pub async fn open_create_root<'a>(b2: &'a b2::B2, roots: &'a mut Vec<BackupRoot>, path: &'a Path)
                                  -> BoxResult<BackupRoot> {
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
                             -> BoxResult<()> {
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
                           -> BoxResult<BackupRoot> {
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
                            -> BoxResult<()> {
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
