use std::path::{Path, PathBuf};
use std::fs;
use std::cmp::Ordering;
use crate::box_result::BoxResult;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct LocalFile {
    pub rel_path: PathBuf,
    pub full_path_hash: String,
    pub last_modified: u64,
    pub mode: u32,
}

#[derive(Eq, Clone)]
pub struct RemoteFile {
    pub rel_path: PathBuf,
    pub full_path_hash: String,
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
               last_modified: u64, mode: u32, is_symlink: bool) -> RemoteFile {
        Self {
            rel_path: filename.to_owned(),
            full_path_hash: fullname.to_owned(),
            id: id.to_string(),
            last_modified,
            mode,
            is_symlink,
        }
    }
}

impl Ord for RemoteFile {
    fn cmp(&self, other: &Self) -> Ordering {
        self.full_path_hash.cmp(&other.full_path_hash)
    }
}

impl PartialOrd for RemoteFile {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.full_path_hash.cmp(&other.full_path_hash))
    }
}

impl PartialEq for RemoteFile {
    fn eq(&self, other: &Self) -> bool {
        self.full_path_hash == other.full_path_hash
    }
}