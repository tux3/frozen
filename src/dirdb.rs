pub mod dirstat;
pub mod pack;
mod bitstream;

use self::dirstat::DirStat;
use std::error::Error;
use std::path::Path;

pub struct DirDB {
    root: DirStat
}

impl DirDB {
    pub fn new_local_db(path: &Path) -> Result<Self, Box<dyn Error>> {
        Ok(Self {
            root: DirStat::new(path)?,
        })
    }
}