use eyre::Result;
use std::fs::Metadata;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::time::UNIX_EPOCH;

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct FileStat {
    pub rel_path: PathBuf,
    pub last_modified: u64,
    pub mode: u32,
}

impl FileStat {
    pub fn new(rel_path: PathBuf, meta: Metadata) -> Result<Self> {
        Ok(FileStat {
            rel_path,
            last_modified: meta.modified()?.duration_since(UNIX_EPOCH).unwrap().as_secs(),
            mode: meta.permissions().mode(),
        })
    }
}
