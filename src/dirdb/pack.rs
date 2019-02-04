use crate::dirdb::DirStat;
use std::error::Error;
use std::io::{Read, Write};
use blake2::VarBlake2b;
use blake2::digest::{Input, VariableOutput};

pub struct PackingInfo {
    need_folder_full_path: bool,
}

fn rebuild_content_hash_from_subfolders(stat: &mut DirStat) -> Result<(), Box<dyn Error>> {
    let mut hasher = VarBlake2b::new(8)?;
    for subfolder in stat.subfolders.iter() {
        hasher.input(&subfolder.content_hash);
    }
    hasher.variable_result(|hash| stat.content_hash.copy_from_slice(hash));
    Ok(())
}

impl DirStat {
    /// Load directory stats from a buffer produced by `serialize_into`
    pub fn new_from_bytes(reader: &mut &[u8]) -> Result<Self, Box<dyn Error>> {
        let subfolders_count = leb128::read::unsigned(reader)?;
        let direct_files_count = leb128::read::unsigned(reader)?;
        let mut stat = Self {
            subfolders: Vec::with_capacity(subfolders_count as usize),
            ..Default::default()
        };

        let mut total_files_count = direct_files_count;
        for _ in 0..subfolders_count {
            let subdir = Self::new_from_bytes(reader)?;
            total_files_count += subdir.total_files_count;
            stat.subfolders.push(subdir);
        }
        stat.total_files_count = total_files_count;

        let dir_name_len = leb128::read::unsigned(reader)?;
        if dir_name_len == 0 {
            reader.read_exact(&mut stat.dir_name_hash)?;
        } else {
            let mut dir_name = vec![0u8; dir_name_len as usize];
            reader.read_exact(dir_name.as_mut())?;
            crate::crypto::raw_hash(&dir_name, 8, &mut stat.dir_name_hash)?;
            stat.dir_name = Some(dir_name);
        }

        if direct_files_count > 0 {
            reader.read_exact(&mut stat.content_hash)?;
        } else {
            rebuild_content_hash_from_subfolders(&mut stat)?;
        }

        Ok(stat)
    }

    /// Serialized the directory stats into a writer.
    /// On error partial data may have been written.
    pub fn serialize_into<W: Write>(&self, writer: &mut W) -> Result<PackingInfo, Box<dyn Error>> {
        let direct_files_count = self.total_files_count - self.subfolders.iter().fold(0, |sum, e|
            sum + e.total_files_count
        );

        leb128::write::unsigned(writer, self.subfolders.len() as u64)?;
        leb128::write::unsigned(writer, direct_files_count)?;

        let mut need_folder_full_path = direct_files_count == 0;
        for subfolder in self.subfolders.iter() {
            let sub_pack_info = subfolder.serialize_into(writer)?;
            need_folder_full_path |= sub_pack_info.need_folder_full_path;
        }
        if self.total_files_count > 0 {
            // Files know their path, so we only need to keep the folder name if we don't have any
            need_folder_full_path = false;
        }

        // We store the name instead of the hash if it's shorter, or if we genuinely need it
        let maybe_write_dir_name = if need_folder_full_path {
            Some(self.dir_name.as_ref().expect("Cannot serialize DirStat without dir names"))
        } else if let Some(dir_name) = &self.dir_name {
            if dir_name.len() < 10 {
                Some(dir_name)
            } else {
                None
            }
        } else {
            None
        };

        if let Some(dir_name) = maybe_write_dir_name {
            leb128::write::unsigned(writer, dir_name.len() as u64)?;
            writer.write_all(&dir_name)?;
        } else {
            leb128::write::unsigned(writer, 0)?;
            writer.write_all(&self.dir_name_hash)?;
        }

        if direct_files_count > 0 {
            writer.write_all(&self.content_hash)?;
        }

        Ok(PackingInfo {
            need_folder_full_path,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::path::Path;
    use crate::dirdb::DirStat;

    #[test]
    fn serialize_roundtrip() -> Result<(), Box<dyn Error>> {
        let stat = DirStat::new(Path::new("test_data/"))?;
        let mut serialized = Vec::new();
        stat.serialize_into(&mut serialized)?;

        let unserialized = DirStat::new_from_bytes(&mut &serialized[..])?;
        assert_eq!(stat, unserialized);

        let mut reserialized = Vec::new();
        unserialized.serialize_into(&mut reserialized)?;
        assert_eq!(serialized, reserialized);
        Ok(())
    }

    #[test]
    fn serialized_size_is_minimal() -> Result<(), Box<dyn Error>> {
        let stat = DirStat {
            total_files_count: u32::max_value() as u64,
            content_hash: [0xAA; 8],
            dir_name: None,
            dir_name_hash: [0xBB; 8],
            subfolders: vec![DirStat{
                total_files_count: 127,
                content_hash: [0xCC; 8],
                dir_name: None,
                dir_name_hash: [0xDD; 8],
                subfolders: Vec::new(),
            }]
        };

        // File count (127) fits in 1 byte, 1+16 bytes of hash, and 1 byte to count the 0 subfolders
        let subfolder_expected_len = 1 + 1 + 8 + 8 + 1;
        let mut subfolder_serialized = Vec::new();
        stat.subfolders[0].serialize_into(&mut subfolder_serialized)?;
        assert_eq!(subfolder_serialized.len(), subfolder_expected_len);

        // 5 LEB128 bytes to count 2^32 files, 1+16 bytes of hash, and room for our 1 subfolder
        let expected_len = 5 + 1 + 8 + 8 + 1 + subfolder_expected_len;
        let mut serialized = Vec::new();
        stat.serialize_into(&mut serialized)?;
        assert_eq!(serialized.len(), expected_len);
        Ok(())
    }
}
