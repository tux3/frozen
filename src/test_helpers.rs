use crate::crypto::Key;
use crate::dirdb::dirstat::DirStat;
use crate::dirdb::filestat::FileStat;
use std::path::PathBuf;

pub use crate::data::root::test_helpers::test_backup_root;
use crate::dirdb::DirDB;
pub use crate::net::b2::test_helpers::test_b2;

pub fn test_key() -> Key {
    Key([0u8; 32])
}

pub fn test_dirstat() -> DirStat {
    DirStat {
        total_files_count: 15,
        direct_files: Some(vec![
            FileStat {
                rel_path: PathBuf::from("a"),
                last_modified: 0,
                mode: 0,
            },
            FileStat {
                rel_path: PathBuf::from("b"),
                last_modified: 0,
                mode: 0,
            },
        ]),
        subfolders: vec![DirStat {
            total_files_count: 5,
            direct_files: Some(vec![FileStat {
                rel_path: PathBuf::from("dir/c"),
                last_modified: 0,
                mode: 0,
            }]),
            subfolders: vec![],
            dir_name: Some("dir".as_bytes().into()),
            dir_name_hash: [5; 8],
            content_hash: [6; 8],
        }],
        dir_name: None,
        dir_name_hash: [0; 8],
        content_hash: [20; 8],
    }
}

pub fn test_dirdb() -> DirDB {
    DirDB { root: test_dirstat() }
}
