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

    pub fn new_from_local(path: &Path) -> Result<Self, Box<dyn Error>> {
        Ok(Self {
            root: DirStat::new(path, path)?,
        })
    }

    pub fn new_from_packed(packed: &[u8], key: &Key) -> Result<Self, Box<dyn Error>> {
        let decrypted = decrypt(packed, key)?;
        Ok(Self {
            root: DirStat::new_from_bytes(&mut decrypted.as_slice())?,
        })
    }

    pub fn to_packed(&self, key: &Key) -> Result<Vec<u8>, Box<dyn Error>> {
        let mut packed_plain = Vec::new();
        self.root.serialize_into(&mut packed_plain)?;
        Ok(encrypt(&packed_plain, key))
    }
}
