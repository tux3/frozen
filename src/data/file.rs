use std::path::{Path, PathBuf};
use std::fs;
use std::cmp::Ordering;
use crate::crypto;
use crate::box_result::BoxResult;
use crate::dirdb::filestat::FileStat;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct LocalFile {
    pub rel_path: PathBuf,
    pub rel_path_hash: String,
    pub last_modified: u64,
    pub mode: u32,
}

#[derive(Eq, Clone)]
pub struct RemoteFile {
    pub rel_path: PathBuf,
    pub rel_path_hash: String,
    pub id: String,
    pub last_modified: u64,
    pub mode: u32,
    pub is_symlink: bool,
}

#[derive(Clone, PartialEq)]
pub struct RemoteFileVersion {
    pub path: String,
    pub id: String,
}

impl LocalFile {
    pub fn from_file_stat(stat: &FileStat, key: &crypto::Key) -> Self {
        Self {
            rel_path: stat.rel_path.clone(),
            rel_path_hash: crypto::hash_path(&stat.rel_path, key),
            last_modified: stat.last_modified,
            mode: stat.mode
        }
    }

    fn full_path(&self, root_path: &Path) -> PathBuf {
        root_path.join(&self.rel_path)
    }

    pub fn is_symlink_at(&self, root_path: &Path) -> BoxResult<bool> {
        Ok(fs::symlink_metadata(self.full_path(root_path))?.file_type().is_symlink())
    }

    pub fn readlink_at(&self, root_path: &Path) -> BoxResult<Vec<u8>> {
        Ok(Vec::from(fs::read_link(self.full_path(root_path))?.to_str().unwrap().as_bytes()))
    }

    pub fn read_all_at(&self, root_path: &Path) -> BoxResult<Vec<u8>> {
        Ok(fs::read(self.full_path(root_path))?)
    }
}

impl RemoteFile {
    pub fn new(filename: &Path, fullname: &str, id: &str,
               last_modified: u64, mode: u32, is_symlink: bool) -> BoxResult<RemoteFile> {
        let elements: Vec<&str> = fullname.split('/').collect();
        if elements.len() != 2 {
            return Err(From::from("Invalid remote file name, expected exactly one slash"));
        }
        Ok(RemoteFile{
            rel_path: filename.to_owned(),
            rel_path_hash: elements[1].to_string(),
            id: id.to_string(),
            last_modified,
            mode,
            is_symlink,
        })
    }
}

impl Ord for RemoteFile {
    fn cmp(&self, other: &Self) -> Ordering {
        self.rel_path_hash.cmp(&other.rel_path_hash)
    }
}

impl PartialOrd for RemoteFile {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.rel_path_hash.cmp(&other.rel_path_hash))
    }
}

impl PartialEq for RemoteFile {
    fn eq(&self, other: &Self) -> bool {
        self.rel_path_hash == other.rel_path_hash
    }
}