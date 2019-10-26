use std::error::Error;
use std::path::{Path, PathBuf};
use std::time::SystemTime;
use blake2::VarBlake2b;
use blake2::digest::{Input, VariableOutput};
use crate::data::paths::path_to_bytes;
use super::FileStat;

#[derive(Default, Debug)]
pub struct DirStat {
    /// This is the total number of files in the tree under this directory
    pub total_files_count: u64,
    /// The files directly in this folder
    pub direct_files: Option<Vec<FileStat>>,
    /// The immediate subfolders of this directory
    pub subfolders: Vec<DirStat>,
    /// This directory's clear name
    pub dir_name: Option<Vec<u8>>,
    /// The hash of the folder name
    pub dir_name_hash: [u8; 8],
    /// Hash of the content's metadata, changes if any file in this folder's tree changes
    pub content_hash: [u8; 8],
}

impl DirStat {
    pub fn new(base_path: &Path, dir_path: &Path) -> Result<Self, Box<dyn Error>> {
        let mut hasher = VarBlake2b::new(8)?;
        let mut total_files_count = 0;
        let mut direct_files = Vec::new();
        let mut subfolders = Vec::new();

        let mut entries = std::fs::read_dir(dir_path)?.filter_map(|e| e.ok()).collect::<Vec<_>>();
        entries.sort_by(|a, b| a.path().cmp(&b.path()));

        for entry in entries {
            let path = entry.path();
            let rel_path = PathBuf::from(path.strip_prefix(base_path)?);
            hasher.input(path_to_bytes(&rel_path).unwrap());
            let is_symlink = entry.file_type().and_then(|ft| Ok(ft.is_symlink())).unwrap_or(false);
            if path.is_dir() && !is_symlink {
                let subfolder = DirStat::new(&base_path, &path)?;
                total_files_count += subfolder.total_files_count;
                hasher.input(&subfolder.content_hash);
                subfolders.push(subfolder);
            } else {
                total_files_count += 1;
                let meta = entry.metadata()?;
                let mtime = meta.modified()?.duration_since(SystemTime::UNIX_EPOCH)?;
                hasher.input(&mtime.as_secs().to_le_bytes());
                hasher.input(&mtime.subsec_nanos().to_le_bytes());
                hasher.input(&meta.len().to_le_bytes());

                direct_files.push(FileStat::new(rel_path, meta)?);
            }
        }

        let dir_name = path_to_bytes(Path::new(dir_path.file_name().unwrap()))?;
        let mut result = Self {
            total_files_count,
            subfolders,
            direct_files: Some(direct_files),
            dir_name: Some(dir_name.to_owned()),
            ..Default::default()
        };
        crate::crypto::raw_hash(&dir_name, 8, &mut result.dir_name_hash)?;
        hasher.variable_result(|hash| result.content_hash.copy_from_slice(hash));
        Ok(result)
    }

    pub fn compute_direct_files_count(&self) -> u64 {
        let subfolder_files_count = self.subfolders.iter().fold(0, |sum, e|
            sum + e.total_files_count
        );
        // File counts may be inaccurate due to pessimistic DirDBs or TOCTOU, could underflow
        self.total_files_count.saturating_sub(subfolder_files_count)
    }
}

impl PartialEq for DirStat {
    fn eq(&self, other: &Self) -> bool {
        self.total_files_count == other.total_files_count
        && self.subfolders == other.subfolders
        && self.dir_name_hash == other.dir_name_hash
        && self.content_hash == other.content_hash
        && self.content_hash != [0; 8]
    }
}

impl Eq for DirStat {}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::path::Path;
    use self::super::DirStat;

    #[test]
    fn count_subfolders() -> Result<(), Box<dyn Error>> {
        let path = Path::new("test_data/Folder A/ac");
        let stat = DirStat::new(path, path)?;
        assert_eq!(stat.subfolders.len(), 1);
        assert_eq!(stat.total_files_count, 2);
        let stat = &stat.subfolders[0]; // ac/aca/
        assert_eq!(stat.subfolders.len(), 1);
        assert_eq!(stat.total_files_count, 1);
        let stat = &stat.subfolders[0]; // ac/aca/acaa/
        assert_eq!(stat.subfolders.len(), 0);
        assert_eq!(stat.total_files_count, 1);
        Ok(())
    }

    #[test]
    fn count_hidden_files() -> Result<(), Box<dyn Error>> {
        // There's two regular files and a file starting with a '.'
        let path = Path::new("test_data/Folder B/");
        assert_eq!(DirStat::new(path, path)?.total_files_count, 3);
        Ok(())
    }

    #[test]
    fn keeps_empty_folders() -> Result<(), Box<dyn Error>> {
        // Subfolders aa/ and ac/ contain files, but ab/ is empty (and kept in Git as a submodule!)
        let path = Path::new("test_data/Folder A");
        assert_eq!(DirStat::new(path, path)?.subfolders.len(), 3);
        Ok(())
    }

    #[test]
    fn count_total_files() -> Result<(), Box<dyn Error>> {
        let path = Path::new("test_data/");
        assert_eq!(DirStat::new(path, path)?.total_files_count, 8);
        Ok(())
    }
}
