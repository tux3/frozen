use crate::crypto;
use crate::data::file::{RemoteFile, RemoteFileVersion};
use crate::net::b2;
use crate::prompt::prompt_yes_no;
use bincode::{deserialize, serialize};
use data_encoding::HEXLOWER_PERMISSIVE;
use eyre::{bail, ensure, eyre, Result};
use serde::{Deserialize, Serialize};
use std::iter::Iterator;
use std::path::{Path, PathBuf};
use std::vec::Vec;

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

    pub async fn list_remote_files(&self, b2: &b2::B2) -> Result<Vec<RemoteFile>> {
        self.list_remote_files_at(b2, "/", b2::FileListDepth::Deep).await
    }

    pub async fn list_remote_files_at(
        &self,
        b2: &b2::B2,
        prefix: &str,
        depth: b2::FileListDepth,
    ) -> Result<Vec<RemoteFile>> {
        ensure!(
            self.lock.is_some(),
            "Cannot list remote files, backup root isn't locked!"
        );

        // We assume the prefix is a relative path hash, starting and ending with /
        debug_assert!(prefix.starts_with('/'));
        debug_assert!(prefix.ends_with('/'));

        let path = self.path_hash.clone() + prefix;
        let mut files = b2.list_remote_files(&path, depth).await?;
        files.sort();
        Ok(files)
    }

    pub async fn lock(&mut self, b2: &b2::B2) -> Result<()> {
        let rand_str = HEXLOWER_PERMISSIVE.encode(&crypto::randombytes(4));
        let lock_path_prefix = self.path_hash.to_owned() + ".lock.";
        let lock_path = lock_path_prefix.to_owned() + &rand_str;

        let lock_version = b2.upload_file_simple(&lock_path, Vec::new()).await?;
        let locks = b2.list_remote_file_versions(&lock_path_prefix).await;
        self.lock = Some((lock_version, b2.clone()));

        if let Err(err) = locks {
            let _ = self.unlock().await;
            return Err(err.wrap_err("Failed to lock backup root"));
        }
        let locks = locks.unwrap();

        if locks.len() > 1 && !prompt_yes_no("Backup root already locked, continue anyways?") {
            let _ = self.unlock().await;
            bail!(
                "Failed to lock the backup root, {} lock already exists",
                locks.len() - 1
            );
        }

        Ok(())
    }

    pub async fn unlock(&mut self) -> Result<()> {
        if self.lock.is_none() {
            return Ok(());
        }
        let (version, b2) = self.lock.take().unwrap();
        b2.delete_file_version(&version).await
    }
}

pub async fn fetch_roots(b2: &b2::B2) -> Result<Vec<BackupRoot>> {
    let enc_data = match b2.download_file("backup_root").await {
        Ok(enc_data) => enc_data,
        Err(_) => return Ok(Vec::new()),
    };
    let data = crypto::decrypt(&enc_data, &b2.key)?;
    Ok(deserialize(&data[..]).unwrap())
}

pub async fn save_roots(b2: &b2::B2, roots: &[BackupRoot]) -> Result<()> {
    let plain_data = serialize(roots)?;
    let data = crypto::encrypt(&plain_data, &b2.key);
    b2.upload_file_simple("backup_root", data).await?;
    Ok(())
}

/// Opens an existing backup root, or creates one if necessary
pub async fn open_create_root(b2: &b2::B2, roots: &mut Vec<BackupRoot>, path: &Path) -> Result<BackupRoot> {
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

pub async fn delete_root(b2: &mut b2::B2, roots: &mut Vec<BackupRoot>, path: &Path) -> Result<()> {
    if roots
        .iter()
        .position(|r| r.path == path)
        .map(|i| roots.remove(i))
        .is_none()
    {
        Err(eyre!(
            "Backup does not exist for \"{}\", nothing to delete",
            path.display()
        ))
    } else {
        save_roots(b2, roots).await
    }
}

/// Opens an existing backup root
pub async fn open_root(b2: &b2::B2, roots: &mut [BackupRoot], path: &Path) -> Result<BackupRoot> {
    match roots.iter().find(|r| r.path == path) {
        Some(root) => {
            let mut root = root.clone();
            root.lock(b2).await?;
            Ok(root)
        }
        None => Err(eyre!("Backup does not exist for \"{}\"", path.display())),
    }
}

/// Forcibly unlocks a backup root
pub async fn wipe_locks(b2: &mut b2::B2, roots: &[BackupRoot], path: &Path) -> Result<()> {
    if let Some(root) = roots.iter().find(|r| r.path == *path) {
        let lock_path_prefix = root.path_hash.to_owned() + ".lock.";
        let locks = b2.list_remote_file_versions(&lock_path_prefix).await?;

        println!("{} lock files to remove", locks.len());
        for lock_version in &locks {
            b2.delete_file_version(lock_version).await?;
        }
        Ok(())
    } else {
        Err(eyre!("Backup does not exist for \"{}\"", path.display()))
    }
}

#[cfg(test)]
pub mod test_helpers {
    use super::BackupRoot;
    use crate::crypto::Key;
    use std::path::Path;

    pub fn test_backup_root(key: &Key) -> BackupRoot {
        BackupRoot::new(Path::new("/tmp/test/path"), key)
    }
}
