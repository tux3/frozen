use std::time::SystemTime;
use std::path::{Path, PathBuf};
use std::fs;
use std::error::Error;
use std::borrow::Cow;

pub struct LocalFile {
    pub rel_path: PathBuf,
    pub last_modified: SystemTime,
}

impl LocalFile {
    pub fn new(base: &Path, path: &Path) -> Result<LocalFile, Box<Error>> {
        Ok(LocalFile {
            rel_path: PathBuf::from(path.strip_prefix(base)?),
            last_modified: fs::metadata(path)?.modified()?,
        })
    }

    pub fn path_str(&self) -> Cow<str> {
        self.rel_path.to_string_lossy()
    }
}