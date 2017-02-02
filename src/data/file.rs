use std::path::{Path, PathBuf};
use std::fs::{self, File};
use std::error::Error;
use std::borrow::Cow;
use std::cmp::Ordering;
use std::io::Read;
use crypto;
use util;

#[derive(Clone)]
pub struct LocalFile {
    pub rel_path: PathBuf,
    pub rel_path_hash: String,
    pub last_modified: u64,
}

#[derive(Eq)]
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

    pub fn read_all(&self, root_path: &String) -> Result<Vec<u8>, Box<Error>> {
        let mut file : File = File::open(root_path.clone()+"/"+&self.path_str())?;
        let mut contents = Vec::new();
        file.read_to_end(&mut contents)?;
        return Ok(contents);
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

    pub fn cmp(&self, other: &LocalFile) -> Ordering {
        self.rel_path_hash.cmp(&other.rel_path_hash)
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