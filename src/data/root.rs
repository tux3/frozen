use std::vec::Vec;
use std::error::Error;
use std::iter::Iterator;
use std::path::Path;
use std::fs;
use bincode;
use bincode::rustc_serialize::{encode, decode};
use crypto;
use b2api;
use super::file::{LocalFile, RemoteFile};

#[derive(Clone, RustcEncodable, RustcDecodable, PartialEq)]
pub struct BackupRoot {
    pub path: String,
    pub path_hash: String,
}

impl BackupRoot {
    fn new(path: &String, key: &crypto::Key) -> BackupRoot {
        BackupRoot {
            path: path.clone(),
            path_hash: crypto::hash_path(path, key),
        }
    }

    pub fn list_local_files(&self, b2: &b2api::B2) -> Result<Vec<LocalFile>, Box<Error>> {
        let path = Path::new(&self.path);
        if !path.is_dir() {
            Err(From::from(format!("{} is not a folder!", &self.path)))
        } else {
            list_local_files(path, path, &b2.key)
        }
    }

    pub fn list_remote_files(&self, b2: &b2api::B2) -> Result<Vec<RemoteFile>, Box<Error>> {
        /// TODO: Use dummy prefix with dummy uploaded files and check multi iterations
        b2api::list_remote_files(b2, &(self.path_hash.clone()+"/"))
    }
}

fn list_local_files(base: &Path, dir: &Path, key: &crypto::Key)
                    -> Result<Vec<LocalFile>, Box<Error>> {
    let mut files = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            files.append(&mut list_local_files(base, &path, key)?);
        } else {
            files.push(LocalFile::new(base, &path, key)?)
        }
    }
    Ok(files)
}

pub fn fetch_roots(b2: &b2api::B2) -> Vec<BackupRoot> {
    let mut roots = Vec::new();

    let root_file_data = b2api::download_file(b2, "backup_root");
    if root_file_data.is_ok() {
        roots = decode(&root_file_data.unwrap()[..]).unwrap();
    }

    return roots;
}

pub fn save_roots(b2: &mut b2api::B2, roots: & mut Vec<BackupRoot>) -> Result<(), Box<Error>> {
    let data = encode(roots, bincode::SizeLimit::Infinite)?;
    b2api::upload_file(b2, "backup_root", &data, None)?;
    Ok(())
}

/// Opens an existing backup root, or creates one if necessary
pub fn open_root(b2: &mut b2api::B2, roots: &mut Vec<BackupRoot>, path: &String)
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

    return Ok(root);
}