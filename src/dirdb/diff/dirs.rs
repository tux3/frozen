use std::collections::hash_map::{HashMap, Entry};
use super::{DirStat};

pub fn merge_dirstats_pessimistic(local: &DirStat, remote: &DirStat) -> DirStat {
    debug_assert!(remote.dir_name_hash == local.dir_name_hash || remote.dir_name_hash == [0; 8]);

    let content_hash = if local.content_hash == remote.content_hash {
        remote.content_hash
    } else {
        [0; 8]
    };
    let mut dirstat = DirStat {
        total_files_count: remote.total_files_count,
        direct_files: None,
        subfolders: Vec::new(),
        dir_name: remote.dir_name.clone(),
        dir_name_hash: remote.dir_name_hash.clone(),
        content_hash,
    };

    let mut local_subdirs = HashMap::new();
    for local_subdir in local.subfolders.iter() {
        local_subdirs.insert(&local_subdir.dir_name_hash, local_subdir);
    }

    for remote_subdir in remote.subfolders.iter() {
        match local_subdirs.entry(&remote_subdir.dir_name_hash) {
            Entry::Occupied(e) => {
                dirstat.subfolders.push(merge_dirstats_pessimistic(e.get(), remote_subdir));
                e.remove();
            },
            Entry::Vacant(_) => {
                dirstat.subfolders.push(pessimize_dirstat(remote_subdir));
            }
        }
    }

    for local_only_subdir in local_subdirs.values() {
        dirstat.subfolders.push(pessimize_dirstat(local_only_subdir));
    }

    dirstat
}

fn pessimize_dirstat(dirstat: &DirStat) -> DirStat {
    DirStat {
        total_files_count: dirstat.total_files_count,
        direct_files: None,
        subfolders: Vec::new(),
        dir_name: dirstat.dir_name.clone(),
        dir_name_hash: dirstat.dir_name_hash,
        content_hash: [0; 8],
    }
}