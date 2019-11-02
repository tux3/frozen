use crate::box_result::BoxResult;
use crate::crypto::{self, Key};
use crate::data::paths::path_from_bytes;
use crate::data::paths::path_to_bytes;
use crate::dirdb::bitstream::*;
use crate::dirdb::DirStat;
use blake2::digest::{Input, VariableOutput};
use blake2::VarBlake2b;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use zstd::stream::{read::Decoder, write::Encoder};

///! Very dense custom bitstream format for DirStat objects
///! We need a dense format because DirStats are uploaded in full after every change,
///! and need to be downloaded before we can start diffing folders.

#[derive(Default)]
struct PackingInfo<'dirstat> {
    need_folder_full_path: bool,
    dir_name: Option<&'dirstat [u8]>,
    subfolders: Vec<PackingInfo<'dirstat>>,
}

struct EncodingSettings {
    file_counts: Encoding,
    subdirs_counts: Encoding,
    dirname_counts: Encoding,
}

fn rebuild_content_hash_from_subfolders(rel_path: &Path, stat: &mut DirStat) -> BoxResult<()> {
    let mut hasher = VarBlake2b::new(8)?;
    for subfolder in stat.subfolders.iter() {
        let sub_name: &Path = path_from_bytes(subfolder.dir_name.as_ref().unwrap())?;
        let sub_rel_path = rel_path.join(sub_name);
        hasher.input(path_to_bytes(&sub_rel_path).unwrap());
        hasher.input(&subfolder.content_hash);
    }
    hasher.variable_result(|hash| stat.content_hash.copy_from_slice(hash));
    Ok(())
}

fn dirnames_packing_info_inner(stat: &DirStat, parent_has_no_files: bool) -> BoxResult<PackingInfo> {
    let mut info = PackingInfo { ..Default::default() };

    let direct_files_count = stat.compute_direct_files_count();

    let no_direct_files = direct_files_count == 0;
    let mut need_folder_full_path = parent_has_no_files;
    for subfolder in stat.subfolders.iter() {
        let sub_pack_info = dirnames_packing_info_inner(subfolder, no_direct_files)?;
        need_folder_full_path |= sub_pack_info.need_folder_full_path;
        info.subfolders.push(sub_pack_info);
    }

    // We store the name instead of the hash if it's short enough, or if we genuinely need it
    // We keep names up to 2x the hash size since they typically compress very well
    info.dir_name = if need_folder_full_path {
        Some(
            stat.dir_name
                .as_ref()
                .expect("Cannot serialize DirStat without dir names")
                .as_slice(),
        )
    } else if stat.dir_name.is_some() && stat.dir_name.as_ref().unwrap().len() > 16 {
        None
    } else {
        stat.dir_name.as_ref().map(|v| v.as_slice())
    };

    Ok(info)
}

/// Collects info to remove subdir names that are too long or unnecessary
fn dirnames_packing_info(stat: &DirStat) -> BoxResult<PackingInfo> {
    let mut info = dirnames_packing_info_inner(stat, false);
    if let Ok(info) = info.as_mut() {
        // The root folder should never serialize its name, it's only the contents we care about.
        info.dir_name = None;
    };
    info
}

fn best_buckets_encoding(buckets: &[usize]) -> Encoding {
    let max_encoding_bits = 2usize.pow(ENCODING_BITS_BITS as u32) - 1;

    let mut use_varint = true;
    let mut best_elem_bits = 8;
    let mut best_total_bits = std::usize::MAX;

    let largest_bucket = buckets.iter().rposition(|&n| n != 0).unwrap();
    if largest_bucket < max_encoding_bits {
        use_varint = false;
        best_elem_bits = largest_bucket;
        best_total_bits = buckets.iter().sum::<usize>() * largest_bucket;
    }

    for varint_bits in 2..=max_encoding_bits {
        let total_varint_bits = buckets.iter().enumerate().fold(0usize, |acc, (val_bits, val_count)| {
            let vals_encoded_bits = if val_bits == 0 {
                varint_bits * val_count
            } else {
                let blocks_per_val = val_bits / (varint_bits - 1) + (val_bits % (varint_bits - 1) != 0) as usize;
                varint_bits * blocks_per_val * val_count
            };
            acc + vals_encoded_bits
        });

        if total_varint_bits <= best_total_bits {
            best_elem_bits = varint_bits;
            best_total_bits = total_varint_bits;
            use_varint = true;
        }
    }

    Encoding {
        use_varint,
        bits: best_elem_bits,
        encoded_data_size: best_total_bits + ENCODING_SIGNALING_OVERHEAD,
    }
}

/// Counts the raw bits required to represent each number, without the 1 bit varint overhead
fn count_bits_required_buckets<T, F, G>(folder: &T, buckets: &mut [usize], get_stat_num: &F, get_subfolders: &G)
where
    F: Fn(&T) -> u64,
    G: Fn(&T) -> &[T],
{
    let num = get_stat_num(folder);
    let bits = f64::log2((num + 1) as f64).ceil() as usize;
    buckets[bits] += 1;

    for subfolder in get_subfolders(folder) {
        count_bits_required_buckets(subfolder, buckets, get_stat_num, get_subfolders);
    }
}

fn best_encoding<T, F, G>(stat: &T, get_stat_num: &F, get_subfolders: &G) -> Encoding
where
    F: Fn(&T) -> u64,
    G: Fn(&T) -> &[T],
{
    let mut buckets = [0usize; 40];
    count_bits_required_buckets(stat, &mut buckets, get_stat_num, get_subfolders);
    best_buckets_encoding(&buckets)
}

/// Tries to find the best varint sizes to use in the bitstream
/// The index of the last nonzero number in buckets is the raw bits required for the largest number
/// If most numbers are in a smaller bucket, a varint of this smaller size will be more efficient
fn best_encoding_settings(stat: &DirStat, info: &PackingInfo) -> EncodingSettings {
    EncodingSettings {
        subdirs_counts: best_encoding(stat, &|stat| stat.subfolders.len() as u64, &|stat| &stat.subfolders[..]),
        file_counts: best_encoding(stat, &|stat| stat.compute_direct_files_count(), &|stat| {
            &stat.subfolders[..]
        }),
        dirname_counts: best_encoding(
            info,
            &|info| match info.dir_name.as_ref() {
                Some(name) => name.len() as u64,
                None => 0,
            },
            &|info| &info.subfolders[..],
        ),
    }
}

impl DirStat {
    // A very internal "how-the-sausage-is-made" type function.
    // The complexity/many arguments are acknowledged and allowed for performance reasons.
    //
    // The path_hash_str/key args are for re-computing the secure dir name hashes as needed
    // (hashes are big, we store the compressed name instead when it turns out to be shorter)
    // The reader args are the separate bitstreams that make up the format, we mux those
    // bitstreams together in a particular (variable, dynamic) order to rebuild the directory tree.
    #[allow(clippy::too_many_arguments)]
    fn subdirs_from_bytes<R: Read>(
        parent_rel_path: Option<&PathBuf>,
        path_hash_str: &mut String,
        key: &Key,
        reader: &mut &[u8],
        files_count_stream: &mut BitstreamReader,
        subdirs_count_stream: &mut BitstreamReader,
        dirname_count_stream: &mut BitstreamReader,
        subdirs_reader: &mut R,
    ) -> BoxResult<Self> {
        let direct_files_count = files_count_stream.read();
        let subfolders_count = subdirs_count_stream.read();
        let dir_name_len = dirname_count_stream.read();
        let mut stat = Self {
            subfolders: Vec::with_capacity(subfolders_count as usize),
            ..Default::default()
        };

        if dir_name_len == 0 {
            reader.read_exact(&mut stat.dir_name_hash)?;
        } else {
            let mut dir_name = vec![0u8; dir_name_len as usize];
            subdirs_reader.read_exact(dir_name.as_mut())?;
            crypto::hash_path_dir_into(path_hash_str, &dir_name, key, &mut stat.dir_name_hash);
            stat.dir_name = Some(dir_name);
        }

        // Skip encoding the dir_name hash for the root folder, its path hash is just "/"
        if !path_hash_str.is_empty() {
            base64::encode_config_buf(&stat.dir_name_hash, base64::URL_SAFE_NO_PAD, path_hash_str);
        }
        path_hash_str.push('/');
        let cur_path_hash_str_len = path_hash_str.len();

        let dir_rel_path = parent_rel_path.and_then(|path| match stat.dir_name.as_ref() {
            None => {
                if path.as_os_str().is_empty() {
                    Some(path.clone())
                } else {
                    None
                }
            }
            Some(dir_name) => {
                let mut sub_path = path.to_owned();
                let subdir_name: &Path = path_from_bytes(&dir_name).unwrap();
                sub_path.push(subdir_name);
                Some(sub_path)
            }
        });

        let mut total_files_count = direct_files_count;
        for _ in 0..subfolders_count {
            path_hash_str.truncate(cur_path_hash_str_len);
            let subdir = Self::subdirs_from_bytes(
                dir_rel_path.as_ref(),
                path_hash_str,
                key,
                reader,
                files_count_stream,
                subdirs_count_stream,
                dirname_count_stream,
                subdirs_reader,
            )?;
            total_files_count += subdir.total_files_count;
            stat.subfolders.push(subdir);
        }
        stat.total_files_count = total_files_count;

        if direct_files_count > 0 {
            reader.read_exact(&mut stat.content_hash)?;
        } else {
            rebuild_content_hash_from_subfolders(&dir_rel_path.unwrap(), &mut stat)?;
        }

        Ok(stat)
    }

    /// Load directory stats from a buffer produced by `serialize_into`
    pub fn new_from_bytes(reader: &mut &[u8], key: &Key) -> BoxResult<Self> {
        let mut files_count_stream = BitstreamReader::new(reader);
        let mut subdirs_count_stream = BitstreamReader::new(files_count_stream.slice_after());
        let mut dirname_count_stream = BitstreamReader::new(subdirs_count_stream.slice_after());

        let mut dirnames_data = dirname_count_stream.slice_after();
        let dirnames_data_size = leb128::read::unsigned(&mut dirnames_data)? as usize;
        let mut dirnames_reader = Decoder::new(dirnames_data)?;

        let subdirs_data = &dirnames_data[dirnames_data_size..];
        let mut path_hash_str = String::new();
        Self::subdirs_from_bytes(
            Some(&PathBuf::new()),
            &mut path_hash_str,
            key,
            &mut &subdirs_data[..],
            &mut files_count_stream,
            &mut subdirs_count_stream,
            &mut dirname_count_stream,
            &mut dirnames_reader,
        )
    }

    fn serialize_dirnames<W: Write>(info: &PackingInfo, writer: &mut W) -> BoxResult<()> {
        if let Some(dir_name) = info.dir_name {
            writer.write_all(&dir_name)?;
        }

        for subfolder in info.subfolders.iter() {
            Self::serialize_dirnames(subfolder, writer)?;
        }

        Ok(())
    }

    fn serialize_subdirs<W: Write>(&self, info: &PackingInfo, writer: &mut W) -> BoxResult<()> {
        let direct_files_count = self.compute_direct_files_count();

        if info.dir_name.is_none() {
            writer.write_all(&self.dir_name_hash)?;
        }

        for (stat_subfolder, info_subfolder) in self.subfolders.iter().zip(info.subfolders.iter()) {
            stat_subfolder.serialize_subdirs(info_subfolder, writer)?;
        }

        if direct_files_count > 0 {
            writer.write_all(&self.content_hash)?;
        }

        Ok(())
    }

    fn serialize_numeric_bitstream<T, F, G, W>(
        folder: &T,
        bitstream_writer: &mut BitstreamWriter<W>,
        get_number: &F,
        get_subfolders: &G,
    ) -> BoxResult<()>
    where
        F: Fn(&T) -> u64,
        G: Fn(&T) -> &[T],
        W: Write,
    {
        bitstream_writer.write(get_number(folder))?;

        for subfolder in get_subfolders(folder) {
            Self::serialize_numeric_bitstream(subfolder, bitstream_writer, get_number, get_subfolders)?;
        }

        Ok(())
    }

    /// Serialized the directory stats into a writer. On error partial data may have been written.
    /// This kind of error is best handled by giving up, the user's machine ain't working today.
    pub fn serialize_into<W: Write>(&self, writer: &mut W) -> BoxResult<()> {
        let packing_info = dirnames_packing_info(self)?;
        let encoding_settings = best_encoding_settings(&self, &packing_info);

        {
            let mut file_count_bitstream_writer = BitstreamWriter::new(writer, encoding_settings.file_counts);
            Self::serialize_numeric_bitstream(
                self,
                &mut file_count_bitstream_writer,
                &|stat| stat.compute_direct_files_count(),
                &|folder| &folder.subfolders[..],
            )?;
        }

        {
            let mut folder_count_bitstream_writer = BitstreamWriter::new(writer, encoding_settings.subdirs_counts);
            Self::serialize_numeric_bitstream(
                self,
                &mut folder_count_bitstream_writer,
                &|stat| stat.subfolders.len() as u64,
                &|folder| &folder.subfolders[..],
            )?;
        }

        {
            let mut dirname_len_bitstream_writer = BitstreamWriter::new(writer, encoding_settings.dirname_counts);
            Self::serialize_numeric_bitstream(
                &packing_info,
                &mut dirname_len_bitstream_writer,
                &|stat| match stat.dir_name.as_ref() {
                    Some(name) => name.len() as u64,
                    None => 0,
                },
                &|folder| &folder.subfolders[..],
            )?;
        }

        let mut dirnames_buf = Vec::new();
        let mut compressor = Encoder::new(&mut dirnames_buf, 22)?;
        Self::serialize_dirnames(&packing_info, &mut compressor)?;
        compressor.finish()?;
        leb128::write::unsigned(writer, dirnames_buf.len() as u64)?;
        writer.write_all(&dirnames_buf)?;

        self.serialize_subdirs(&packing_info, writer)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::box_result::BoxResult;
    use crate::crypto::Key;
    use crate::dirdb::DirStat;
    use std::path::Path;

    #[test]
    fn serialize_roundtrip() -> BoxResult<()> {
        let path = Path::new("test_data");

        let mut stat = DirStat::new(path, path)?;
        let mut path_hash_str = "/".to_string();
        let key = Key([0; 32]);
        stat.recompute_dir_name_hashes(&mut path_hash_str, &key);

        let mut serialized = Vec::new();
        stat.serialize_into(&mut serialized)?;

        let unserialized = DirStat::new_from_bytes(&mut &serialized[..], &key)?;
        assert_eq!(stat, unserialized);

        let mut reserialized = Vec::new();
        unserialized.serialize_into(&mut reserialized)?;
        assert_eq!(serialized, reserialized);
        Ok(())
    }
}
