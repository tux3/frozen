use std::error::Error;
use std::path::Path;
use std::time::SystemTime;
use blake2::VarBlake2b;
use blake2::digest::{Input, VariableOutput};
use bincode::serialize;

pub struct DirStat {
    /// This is the total number of files in the tree under this directory
    pub total_files_count: u64,
    /// The immediate subfolders of this directory
    pub subfolders: Vec<DirStat>,
    /// The hash of the folder path, relative to the backup root
    pub dir_path_hash: [u8; 8],
    /// Hash of the content's metadata, changes if any file in this folder's tree changes
    pub content_hash: [u8; 8],
}

impl DirStat {
    pub fn new(dir_path: &Path) -> Result<Self, Box<dyn Error>> {
        let mut hasher = VarBlake2b::new(8)?;
        let mut total_files_count = 0;
        let mut subfolders = Vec::new();

        let mut entries = std::fs::read_dir(dir_path)?.filter_map(|e| e.ok()).collect::<Vec<_>>();
        entries.sort_by(|a, b| a.path().cmp(&b.path()));

        for entry in entries {
            let path = entry.path();
            let is_symlink = entry.file_type().and_then(|ft| Ok(ft.is_symlink())).unwrap_or(false);
            if path.is_dir() && !is_symlink {
                let subfolder = DirStat::new(&path)?;
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
            }
        }
        
        let mut result = Self {
            total_files_count,
            subfolders,
            dir_path_hash: [0; 8],
            content_hash: [0; 8],
        };
        crate::crypto::raw_hash(&serialize(dir_path)?, 8, &mut result.dir_path_hash)?;
        hasher.variable_result(|hash| result.content_hash.copy_from_slice(hash));
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::path::Path;
    use self::super::DirStat;

    #[test]
    fn count_subfolders() -> Result<(), Box<dyn Error>> {
        let stat = DirStat::new(Path::new("test_data/Folder A/ac"))?;
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
        assert_eq!(DirStat::new(Path::new("test_data/Folder B/"))?.total_files_count, 3);
        Ok(())
    }

    #[test]
    fn keeps_empty_folders() -> Result<(), Box<dyn Error>> {
        // Subfolders aa/ and ac/ contain files, but ab/ is empty (and kept in Git as a submodule!)
        assert_eq!(DirStat::new(Path::new("test_data/Folder A"))?.subfolders.len(), 3);
        Ok(())
    }

    #[test]
    fn count_total_files() -> Result<(), Box<dyn Error>> {
        assert_eq!(DirStat::new(Path::new("test_data/"))?.total_files_count, 8);
        Ok(())
    }
}
