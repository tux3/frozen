use super::{DirStat, FileDiffStream};
use crate::data::root::BackupRoot;
use crate::dirdb::DirDB;
use crate::net::b2::B2;
use futures::stream::SelectAll;
use owning_ref::ArcRef;
use std::collections::hash_map::{Entry, HashMap};
use std::sync::Arc;

struct DiffTree {
    children: Vec<DiffTree>,
    local: Option<ArcRef<DirDB, DirStat>>,
    prefix_path_hash: String,
    total_files_count: u64,  // How many (remote) files we can expect a deep list request to return
    direct_files_count: u64, // How many (remote) files a shallow list request is expected to return
    deep_diff: bool,         // If false, we do a shallow list request of just the folder's direct files
    local_only: bool,        // If true, the folder doesn't exist on the remote
}

fn optimized_diff_tree(local: ArcRef<DirDB, DirStat>, remote: &DirStat) -> Option<DiffTree> {
    let mut prefix_path_hash = "/".to_owned();

    // When the remote DB is empty/missing, or pessimized and with no folders, deep-diff everything
    // Normally we deep-diff when a remote folder is missing locally, but this is the root folder,
    // and, a root with no subdirs is just as fast to deep-diff as shallow-diff, so we don't lose
    // any performance by sharing the same encoding for "no subdirs at all" and "no dirdb at all"
    if remote.content_hash == [0u8; 8] && remote.subfolders.is_empty() {
        return Some(DiffTree {
            children: vec![],
            local: Some(local),
            prefix_path_hash,
            direct_files_count: 0,
            total_files_count: 0,
            deep_diff: true,
            local_only: false,
        });
    }

    let tree = DiffTree::new(&mut prefix_path_hash, &local, &remote);
    tree.and_then(|mut tree| {
        tree.optimize();
        Some(tree)
    })
}

pub fn diff_dirs(
    root: Arc<BackupRoot>,
    b2: Arc<B2>,
    local: ArcRef<DirDB, DirStat>,
    remote: &DirStat,
) -> SelectAll<FileDiffStream> {
    let mut diff_streams = SelectAll::new();
    let diff_tree = match optimized_diff_tree(local, &remote) {
        None => return diff_streams, // If nothing changed, we can take the fast way out
        Some(t) => t,
    };

    diff_tree.into_diff_streams(root, b2, &mut diff_streams);
    diff_streams
}

impl DiffTree {
    pub fn new(prefix_path_hash: &mut String, local: &ArcRef<DirDB, DirStat>, remote: &DirStat) -> Option<Self> {
        debug_assert!(remote.dir_name_hash == local.dir_name_hash);
        debug_assert!(prefix_path_hash.starts_with('/'));
        debug_assert!(prefix_path_hash.ends_with('/'));
        if local.content_hash == remote.content_hash {
            return None;
        }

        let mut tree = DiffTree {
            children: Vec::new(),
            local: Some(ArcRef::clone(local)),
            prefix_path_hash: prefix_path_hash.clone(),
            total_files_count: remote.total_files_count,
            direct_files_count: remote.total_files_count, // Updated in loop below
            deep_diff: false,
            local_only: false,
        };

        let cur_prefix_path_hash_len = prefix_path_hash.len();

        let mut local_subdirs = HashMap::new();
        for (index, local_subdir) in local.subfolders.iter().enumerate() {
            local_subdirs.insert(
                &local_subdir.dir_name_hash,
                local.clone().map(|dir| &dir.subfolders[index]),
            );
        }

        for remote_subdir in remote.subfolders.iter() {
            prefix_path_hash.truncate(cur_prefix_path_hash_len);
            base64::encode_config_buf(&remote_subdir.dir_name_hash, base64::URL_SAFE_NO_PAD, prefix_path_hash);
            prefix_path_hash.push('/');

            tree.direct_files_count -= remote_subdir.total_files_count;

            match local_subdirs.entry(&remote_subdir.dir_name_hash) {
                Entry::Occupied(e) => {
                    if let Some(subtree) = DiffTree::new(prefix_path_hash, e.get(), remote_subdir) {
                        tree.children.push(subtree);
                    }
                    e.remove();
                }
                Entry::Vacant(_) => {
                    tree.children.push(DiffTree {
                        children: Vec::new(),
                        local: None,
                        prefix_path_hash: prefix_path_hash.clone(),
                        total_files_count: remote_subdir.total_files_count,
                        direct_files_count: remote_subdir.total_files_count, // Largely meaningless!
                        deep_diff: true, // Could have subfolders, but they're not gonna be in the tree
                        local_only: false,
                    });
                }
            }
        }

        for (_hash, local_only_subdir) in local_subdirs.into_iter() {
            prefix_path_hash.truncate(cur_prefix_path_hash_len);
            base64::encode_config_buf(
                &local_only_subdir.dir_name_hash,
                base64::URL_SAFE_NO_PAD,
                prefix_path_hash,
            );
            prefix_path_hash.push('/');

            tree.children.push(DiffTree {
                children: Vec::new(),
                local: Some(local_only_subdir),
                prefix_path_hash: prefix_path_hash.clone(),
                total_files_count: 0, // Nothing to list if the remote folder doesn't exist
                direct_files_count: 0,
                deep_diff: false,
                local_only: true,
            });
        }

        Some(tree)
    }

    /// How many requests it costs to list this many files
    fn files_count_to_request_cost(files_count: u64) -> u64 {
        const MAX_FILES_PER_REQUEST: u64 = 1000;

        // Division rounding up (with x86 in mind, which has fast division+remainder)
        let div = files_count / MAX_FILES_PER_REQUEST;
        let rem = files_count % MAX_FILES_PER_REQUEST;
        // NOTE: We don't eliminate requests with 0 expected remote files at the moment
        if rem != 0 || files_count == 0 {
            div + 1
        } else {
            div
        }
    }

    /// Optimizes how many requests are needed to diff this tree, and returns the number
    fn optimize_with_costs(&mut self) -> u64 {
        // We don't make *any* requests for local-only folders (but they're still part of the tree)
        if self.local_only {
            debug_assert!(self.children.is_empty());
            return 0;
        }

        let merged_diff_cost = Self::files_count_to_request_cost(self.total_files_count);
        let mut separate_diff_cost = 0;
        for subtree in self.children.iter_mut() {
            separate_diff_cost += subtree.optimize_with_costs();
        }

        if self.deep_diff {
            // If we're deep-diffing, we don't want any sub-requests (but we CAN have local-only subfolders!)
            debug_assert_eq!(separate_diff_cost, 0);
            return merged_diff_cost; // A smart compiler would move that before the loop...
        }
        separate_diff_cost += Self::files_count_to_request_cost(self.direct_files_count);

        if merged_diff_cost < separate_diff_cost {
            self.deep_diff = true;
            self.children.clear();
            merged_diff_cost
        } else {
            separate_diff_cost
        }
    }

    pub fn optimize(&mut self) {
        self.optimize_with_costs();
    }

    pub fn into_diff_streams(self, root: Arc<BackupRoot>, b2: Arc<B2>, diff_streams: &mut SelectAll<FileDiffStream>) {
        let stream = match (self.local, self.local_only) {
            (local, false) => FileDiffStream::new(
                root.clone(),
                b2.clone(),
                self.prefix_path_hash.clone(),
                local,
                self.deep_diff,
            ),
            (Some(local), true) => {
                FileDiffStream::new_local(root.clone(), self.prefix_path_hash.clone(), local, &b2.key)
            }
            (None, true) => unreachable!("We can't have a local-only folder without a local DirStat!"),
        };
        diff_streams.push(stream);

        for child in self.children.into_iter() {
            child.into_diff_streams(root.clone(), b2.clone(), diff_streams);
        }
    }
}

pub fn merge_dirstats_pessimistic(local: &DirStat, remote: &DirStat) -> DirStat {
    debug_assert!(remote.dir_name_hash == local.dir_name_hash);
    if remote.dir_name.is_some() {
        debug_assert_eq!(local.dir_name, remote.dir_name);
    }

    let content_hash = if local.content_hash == remote.content_hash {
        remote.content_hash
    } else {
        [0; 8]
    };
    let mut dirstat = DirStat {
        total_files_count: remote.total_files_count,
        direct_files: None,
        subfolders: Vec::new(),
        dir_name: local.dir_name.clone(),
        dir_name_hash: local.dir_name_hash,
        content_hash,
    };

    let mut local_subdirs = HashMap::new();
    for local_subdir in local.subfolders.iter() {
        local_subdirs.insert(&local_subdir.dir_name_hash, local_subdir);
    }

    for remote_subdir in remote.subfolders.iter() {
        match local_subdirs.entry(&remote_subdir.dir_name_hash) {
            Entry::Occupied(e) => {
                let pessimized = merge_dirstats_pessimistic(e.get(), remote_subdir);

                // Account for the subdir file count change (avoiding casts & u64 underflow ...)
                dirstat.total_files_count += pessimized.total_files_count;
                dirstat.total_files_count -= remote_subdir.total_files_count;

                dirstat.subfolders.push(pessimized);
                e.remove();
            }
            Entry::Vacant(_) => {
                // NOTE: We do NOT substract this folder's total_files_count from the parent's
                // because if the subfolder keeps its total file count then substracting it
                // from the parent would make the direct_files_count calculation incorrect,
                // because the latter assumes *all* subfolders have their exact total_files_count.
                dirstat.subfolders.push(pessimize_dirstat(remote_subdir));
            }
        }
    }

    for local_only_subdir in local_subdirs.values() {
        dirstat.total_files_count += local_only_subdir.total_files_count;
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

#[cfg(test)]
mod test {
    use crate::dirdb::diff::dirs::{diff_dirs, optimized_diff_tree, DiffTree};
    use crate::dirdb::DirDB;
    use crate::test_helpers::*;
    use owning_ref::ArcRef;
    use std::sync::Arc;

    impl DiffTree {
        /// A shallow-diffed folder, can have indirect files (for folders not in the diff tree)
        /// Indirect files are unchanged, they add pure cost to merging nodes of the tree
        fn new_without_subdirs(indirect_files: u64, direct_files: u64) -> DiffTree {
            DiffTree {
                children: vec![],
                local: None,
                prefix_path_hash: "/".to_string(),
                total_files_count: indirect_files + direct_files,
                direct_files_count: direct_files,
                deep_diff: false,
                local_only: false,
            }
        }

        /// A deep-diffed folder, with simple fixed cost
        fn new_deep_diffed(total_files: u64) -> DiffTree {
            DiffTree {
                children: vec![],
                local: None,
                prefix_path_hash: "/".to_string(),
                total_files_count: total_files,
                direct_files_count: total_files,
                deep_diff: true,
                local_only: false,
            }
        }

        // Remember to update any parent folders
        fn add_subfolders(&mut self, count: usize, file_counts: fn() -> u64) {
            for _ in 0..count {
                let subfolder_files_count = file_counts();
                self.total_files_count += subfolder_files_count;
                let subfolder = DiffTree {
                    children: vec![],
                    local: None,
                    prefix_path_hash: "".to_string(),
                    total_files_count: subfolder_files_count,
                    direct_files_count: subfolder_files_count,
                    deep_diff: false,
                    local_only: false,
                };
                self.children.push(subfolder)
            }
        }

        fn move_to_parent(self, parent: &mut DiffTree) {
            parent.total_files_count += self.total_files_count;
            parent.children.push(self);
        }

        /// The new parent can have unchanged indirect files in subfolders we don't want to diff
        fn wrap_in_new_parent(self, indirect_files: u64) -> DiffTree {
            let mut parent = DiffTree::new_without_subdirs(indirect_files, 0);
            self.move_to_parent(&mut parent);
            parent
        }
    }

    #[test]
    fn empty_remote_dirdb() {
        // If there's no remote DirDB (or invalid/empty), we must diff everything
        let key = test_key();
        let b2 = Arc::new(test_b2(key.clone()));
        let root = Arc::new(test_backup_root(&key));
        let local = ArcRef::new(Arc::new(test_dirdb())).map(|d| &d.root);
        let remote = DirDB::new_empty();

        let streams = diff_dirs(root, b2, local.clone(), &remote.root);
        assert_eq!(streams.len(), 1); // Exactly one diff stream: everything

        let tree = optimized_diff_tree(local, &remote.root).unwrap();
        assert!(tree.children.is_empty());
        assert!(tree.prefix_path_hash == "/");
        assert!(tree.deep_diff);
        assert!(!tree.local_only);
        assert!(!tree.local.is_none());
    }

    #[test]
    fn simple_merge_up() {
        // This heavy subdir's parents have few other files, so we can deep-diff them directly
        let tree = DiffTree::new_without_subdirs(4200, 250);
        let mut tree = tree.wrap_in_new_parent(50);
        tree.add_subfolders(3, || 150);
        let mut root = tree.wrap_in_new_parent(0);
        let cost = root.optimize_with_costs();

        // We expect a single deep-diff request at the root
        let expected_cost = DiffTree::files_count_to_request_cost(root.total_files_count);

        assert_eq!(cost, expected_cost);
        assert!(root.children.is_empty());
        assert!(root.deep_diff);
        assert_eq!(root.prefix_path_hash, "/");
    }

    #[test]
    fn simple_merge_siblings() {
        // Either subdir alone is too big to merge with the parent,
        // but everything together is better than everything separately
        let mut root = DiffTree::new_without_subdirs(700, 1);
        DiffTree::new_deep_diffed(400).move_to_parent(&mut root);
        DiffTree::new_deep_diffed(400).move_to_parent(&mut root);
        let cost = root.optimize_with_costs();

        // We expect a single deep-diff request at the root
        let expected_cost = DiffTree::files_count_to_request_cost(root.total_files_count);

        assert_eq!(cost, expected_cost);
        assert!(root.children.is_empty());
        assert!(root.deep_diff);
    }

    #[test]
    fn simple_dont_merge_siblings() {
        // Either subdir alone is too big to merge with the parent,
        // but everything together is better than everything separately
        let mut root = DiffTree::new_without_subdirs(50, 1);
        DiffTree::new_without_subdirs(1000, 15).move_to_parent(&mut root);
        DiffTree::new_without_subdirs(1000, 15).move_to_parent(&mut root);
        let cost = root.optimize_with_costs();

        // We expect only shallow diffs, that's the root plus 2 subfolders
        let expected_cost = 1 + 2;

        assert_eq!(cost, expected_cost);
        assert_eq!(root.children.len(), 2);
        assert!(!root.deep_diff);
    }

    #[test]
    fn merge_up_to_heavy_subfolder() {
        // This is a deeper hierarchy, but merging up has to stop due to a heavy subfolder
        let tree = DiffTree::new_without_subdirs(4200, 1);
        let mut tree = tree.wrap_in_new_parent(0);
        tree.add_subfolders(4, || 25);
        let mut tree = tree.wrap_in_new_parent(10);
        tree.add_subfolders(2, || 0);
        let tree = tree.wrap_in_new_parent(5);
        let expected_subdir_cost = DiffTree::files_count_to_request_cost(tree.total_files_count);

        // This has a subfolder heavy to merge with the other folder, without it we'd be fine
        let mut parent = DiffTree::new_without_subdirs(99999, 1).wrap_in_new_parent(1);
        tree.move_to_parent(&mut parent);
        let mut root = parent.wrap_in_new_parent(1);
        let cost = root.optimize_with_costs();

        // We expect to deep-diff the subdir, and shallow diff the heavy subdir, plus the parent and the root
        let expected_cost = expected_subdir_cost + 1 + 1 + 1;
        assert_eq!(cost, expected_cost);
        assert!(!root.deep_diff);
        assert_eq!(root.children.len(), 1);

        let parent = &root.children[0];
        assert!(!parent.deep_diff);
        assert_eq!(parent.children.len(), 2);

        assert!(!parent.children[0].deep_diff);
        assert!(parent.children[1].deep_diff);
    }

    #[test]
    fn merge_ignoring_local_only_folders() {
        // The local-only folders should not prevent merge, and should go away since we deep-diff
        let tree = DiffTree::new_without_subdirs(0, 2000);
        let mut tree = tree.wrap_in_new_parent(0);
        let mut local_only = DiffTree::new_without_subdirs(0, 0);
        local_only.local_only = true;
        local_only.move_to_parent(&mut tree);

        let mut tree = tree.wrap_in_new_parent(2000).wrap_in_new_parent(1);
        tree.add_subfolders(4, || 25);
        let mut tree = tree.wrap_in_new_parent(10);
        tree.add_subfolders(2, || 0);
        let tree = tree.wrap_in_new_parent(5);
        let mut root = tree.wrap_in_new_parent(1);
        let cost = root.optimize_with_costs();

        // We expect everything to merge, even though there's a local folder that needs preserving
        let expected_cost = DiffTree::files_count_to_request_cost(root.total_files_count);

        assert_eq!(cost, expected_cost);
        assert!(root.deep_diff);
        assert!(root.children.is_empty());
    }

    #[test]
    fn very_large_shallow_diff_doesnt_merge_up() {
        let tree = DiffTree::new_without_subdirs(99999, 1);
        let tree = tree.wrap_in_new_parent(1);
        let tree = tree.wrap_in_new_parent(1);
        let mut root = tree.wrap_in_new_parent(1);
        let cost = root.optimize_with_costs();

        // We expect a shallow request per subfolder
        let expected_cost = 4;
        assert_eq!(cost, expected_cost);
        assert!(!root.deep_diff);
    }

    #[test]
    fn moderately_large_shallow_diff_can_eventually_merge_up() {
        let tree = DiffTree::new_without_subdirs(3900, 1);
        let tree = tree.wrap_in_new_parent(1);
        let tree = tree.wrap_in_new_parent(1);
        let tree = tree.wrap_in_new_parent(1);
        let tree = tree.wrap_in_new_parent(1);
        let mut root = tree.wrap_in_new_parent(1);
        let cost = root.optimize_with_costs();

        // Merging is a fixed 4 requests, not merging becomes more expensive with depth
        // We expect a merge eventually as we walk back up the tree
        let expected_cost = 4;
        assert_eq!(cost, expected_cost);
        assert!(root.deep_diff);
    }
}
