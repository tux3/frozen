use std::time::SystemTime;
use std::path::{Path, PathBuf};
use std::fs;
use std::str::FromStr;
use std::error::Error;
use std::borrow::Cow;
use crypto;
use util;

pub struct LocalFile {
    pub rel_path: PathBuf,
    pub rel_path_hash: String,
    pub last_modified: u64,
}

pub struct RemoteFile {
    pub rel_path_hash: String,
    pub last_modified: u64,
}

impl LocalFile {
    pub fn new(base: &Path, path: &Path, key: &crypto::Key) -> Result<LocalFile, Box<Error>> {
        let rel_path = PathBuf::from(path.strip_prefix(base)?);
        Ok(LocalFile {
            rel_path_hash: crypto::hash_path(&rel_path.to_string_lossy().to_string(), key),
            rel_path: rel_path,
            last_modified: util::to_timestamp(fs::metadata(path)?.modified()?),
        })
    }

    pub fn path_str(&self) -> Cow<str> {
        self.rel_path.to_string_lossy()
    }
}

impl RemoteFile {
    pub fn new(fullname: &str, last_modified: u64) -> Result<RemoteFile, Box<Error>> {
        let elements: Vec<&str> = fullname.split('/').collect();
        if elements.len() != 2 {
            return Err(From::from("Invalid remote file name, expected exactly one slash"));
        }
        Ok(RemoteFile{
            rel_path_hash: elements[1].to_string(),
            last_modified: last_modified,
        })
    }
}