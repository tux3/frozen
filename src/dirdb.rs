pub mod dirstat;
pub mod filestat;
pub mod pack;
pub mod diff;
mod bitstream;

use self::dirstat::DirStat;
use self::filestat::FileStat;

use crate::crypto::{Key, encrypt, decrypt};
use std::error::Error;
use std::path::Path;
use crate::box_result::BoxResult;

pub struct DirDB {
    root: DirStat
}

impl DirDB {
    pub fn new_empty() -> Self {
        DirDB {
            root: DirStat {
                total_files_count: 0,
                direct_files: None,
                subfolders: Vec::new(),
                dir_name: None,
                dir_name_hash: [0; 8],
                content_hash: [0; 8],
            }
        }
    }

    pub fn new_from_local(path: &Path) -> BoxResult<Self> {
        let mut root = DirStat::new(path, path)?;

        // It'd be meaningless for the root dir to have a name relative to itself!
        root.dir_name = None;
        root.dir_name_hash = [0; 8];

        Ok(Self {
            root,
        })
    }

    pub fn new_from_packed(packed: &[u8], key: &Key) -> BoxResult<Self> {
        let decrypted = decrypt(packed, key)?;
        Ok(Self {
            root: DirStat::new_from_bytes(&mut decrypted.as_slice())?,
        })
    }

    pub fn to_packed(&self, key: &Key) -> BoxResult<Vec<u8>> {
        let mut packed_plain = Vec::new();
        self.root.serialize_into(&mut packed_plain)?;
        Ok(encrypt(&packed_plain, key))
    }
}
