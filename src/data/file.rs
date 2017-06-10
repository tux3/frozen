use std::path::{Path, PathBuf};
use std::fs::{self, File};
use std::error::Error;
use std::borrow::Cow;
use std::cmp::Ordering;
use std::io::{Read, Seek, SeekFrom};
use std::os::unix::fs::PermissionsExt;
use crypto;
use util;

#[derive(Clone)]
pub struct LocalFile {
    pub rel_path: PathBuf,
    pub rel_path_hash: String,
    pub last_modified: u64,
    pub mode: u32,
}

#[derive(Eq, Clone)]
pub struct RemoteFile {
    pub rel_path: String,
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
    pub fn new(base: &Path, path: &Path, key: &crypto::Key) -> Result<LocalFile, Box<Error>> {
        let rel_path = PathBuf::from(path.strip_prefix(base)?);
        let meta = fs::symlink_metadata(path)?;
        Ok(LocalFile {
            rel_path_hash: crypto::hash_path(&rel_path.to_string_lossy().to_string(), key),
            rel_path: rel_path,
            mode: meta.permissions().mode(),
            last_modified: util::to_timestamp(meta.modified()?),
        })
    }

    pub fn path_str(&self) -> Cow<str> {
        self.rel_path.to_string_lossy()
    }

    pub fn is_symlink(&self, root_path: &str) -> Result<bool, Box<Error>> {
        let fullpath = root_path.to_owned()+"/"+&self.path_str();
        Ok(fs::symlink_metadata(fullpath)?.file_type().is_symlink())
    }

    pub fn readlink(&self, root_path: &str) -> Result<Vec<u8>, Box<Error>> {
        let fullpath = root_path.to_owned()+"/"+&self.path_str();
        Ok(Vec::from(fs::read_link(fullpath)?.to_str().unwrap().as_bytes()))
    }

    pub fn read_all(&self, root_path: &str) -> Result<Vec<u8>, Box<Error>> {
        let mut file : File = File::open(root_path.to_owned()+"/"+&self.path_str())?;
        let mut size = file.seek(SeekFrom::End(0))? as usize;
        file.seek(SeekFrom::Start(0))?;
        let mut contents = Vec::with_capacity(size);
        unsafe { contents.set_len(size); }
        let mut pos = 0;
        while let Ok(n) = file.read(&mut contents[pos..pos+size]) {
            if n == 0 {
                break;
            }
            pos += n;
            size -= n;
        }
        Ok(contents)
    }
}

impl RemoteFile {
    pub fn new(filename: &str, fullname: &str, id: &str,
               last_modified: u64, mode: u32, is_symlink: bool)
            -> Result<RemoteFile, Box<Error>> {
        let elements: Vec<&str> = fullname.split('/').collect();
        if elements.len() != 2 {
            return Err(From::from("Invalid remote file name, expected exactly one slash"));
        }
        Ok(RemoteFile{
            rel_path: filename.to_string(),
            rel_path_hash: elements[1].to_string(),
            id: id.to_string(),
            last_modified,
            mode,
            is_symlink,
        })
    }

    pub fn cmp_local(&self, other: &LocalFile) -> Ordering {
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