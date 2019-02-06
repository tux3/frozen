use crate::dirdb::bitstream::*;
use crate::dirdb::DirStat;
use std::error::Error;
use std::io::{Read, Write};
use blake2::VarBlake2b;
use blake2::digest::{Input, VariableOutput};
use zstd::stream::{write::Encoder, read::Decoder};

///! Very dense custom bitstream format for DirStat objects
///! We need a dense format because DirStats are uploaded in full after every change,
///! and need to be downloaded before we can start diffing folders.

struct PackingInfo {
    need_folder_full_path: bool,
}

struct EncodingSettings {
    file_counts: Encoding,
    subdirs_counts: Encoding,
    dirname_counts: Encoding,
}

fn rebuild_content_hash_from_subfolders(stat: &mut DirStat) -> Result<(), Box<dyn Error>> {
    let mut hasher = VarBlake2b::new(8)?;
    for subfolder in stat.subfolders.iter() {
        hasher.input(&subfolder.content_hash);
    }
    hasher.variable_result(|hash| stat.content_hash.copy_from_slice(hash));
    Ok(())
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
                let blocks_per_val = val_bits / (varint_bits-1) + (val_bits % (varint_bits-1) != 0) as usize;
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
fn count_bits_required_buckets<F>(stat: &DirStat, buckets: &mut [usize], get_stat_num: &F)
    where F: Fn(&DirStat) -> u64 {
    let num = get_stat_num(stat);
    let bits = f64::log2((num+1) as f64).ceil() as usize;
    buckets[bits] += 1;

    for subfolder in stat.subfolders.iter() {
        count_bits_required_buckets(&subfolder, buckets, get_stat_num);
    }
}

fn best_encoding<F>(stat: &DirStat, get_stat_num: &F) -> Encoding
    where F: Fn(&DirStat) -> u64 {
    let mut buckets = [0usize; 40];
    count_bits_required_buckets(stat, &mut buckets, get_stat_num);
    best_buckets_encoding(&buckets)
}

/// Tries to find the best varint sizes to use in the bitstream
/// The index of the last nonzero number in buckets is the raw bits required for the largest number
/// If most numbers are in a smaller bucket, a varint of this smaller size will be more efficient
fn best_encoding_settings(stat: &DirStat) -> EncodingSettings {
    EncodingSettings {
        subdirs_counts: best_encoding(stat, &|stat| stat.subfolders.len() as u64),
        file_counts: best_encoding(stat, &|stat| {
            stat.total_files_count - stat.subfolders.iter().fold(0, |sum, e|
                sum + e.total_files_count
            )
        }),
        dirname_counts: best_encoding(stat, &|stat| {
            match stat.dir_name.borrow().as_ref() {
                Some(name) => name.len() as u64,
                None => 0,
            }
        }),
    }
}

impl DirStat {
    fn subdirs_from_bytes<R: Read>(reader: &mut &[u8],
                                  files_count_stream: &mut BitstreamReader,
                                  subdirs_count_stream: &mut BitstreamReader,
                                  dirname_count_stream: &mut BitstreamReader,
                                  subdirs_reader: &mut R) -> Result<Self, Box<dyn Error>> {
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
            crate::crypto::raw_hash(&dir_name, 8, &mut stat.dir_name_hash)?;
            stat.dir_name.replace(Some(dir_name));
        }

        let mut total_files_count = direct_files_count;
        for _ in 0..subfolders_count {
            let subdir = Self::subdirs_from_bytes(reader,
                                                  files_count_stream,
                                                  subdirs_count_stream,
                                                  dirname_count_stream,
                                                  subdirs_reader)?;
            total_files_count += subdir.total_files_count;
            stat.subfolders.push(subdir);
        }
        stat.total_files_count = total_files_count;

        if direct_files_count > 0 {
            reader.read_exact(&mut stat.content_hash)?;
        } else {
            rebuild_content_hash_from_subfolders(&mut stat)?;
        }

        Ok(stat)
    }

    /// Load directory stats from a buffer produced by `serialize_into`
    pub fn new_from_bytes(reader: &mut &[u8]) -> Result<Self, Box<dyn Error>> {
        let mut files_count_stream =  BitstreamReader::new(reader);
        let mut subdirs_count_stream =  BitstreamReader::new(files_count_stream.slice_after());
        let mut dirname_count_stream =  BitstreamReader::new(subdirs_count_stream.slice_after());

        let mut dirnames_data = dirname_count_stream.slice_after();
        let dirnames_data_size = leb128::read::unsigned(&mut dirnames_data)? as usize;
        let mut dirnames_reader = Decoder::new(dirnames_data)?;

        let subdirs_data = &dirnames_data[dirnames_data_size..];
        Self::subdirs_from_bytes(&mut &subdirs_data[..],
                                 &mut files_count_stream,
                                 &mut subdirs_count_stream,
                                 &mut dirname_count_stream,
                                 &mut dirnames_reader)
    }

    fn serialize_dirnames<W: Write>(&self, writer: &mut W) -> Result<(), Box<dyn Error>> {
        if let Some(dir_name) = self.dir_name.borrow().as_ref() {
            writer.write_all(&dir_name)?;
        }

        for subfolder in self.subfolders.iter() {
            subfolder.serialize_dirnames(writer)?;
        }

        Ok(())
    }

    fn serialize_subdirs<W: Write>(&self, writer: &mut W) -> Result<(), Box<dyn Error>> {
        let direct_files_count = self.total_files_count - self.subfolders.iter().fold(0, |sum, e|
            sum + e.total_files_count
        );

        if self.dir_name.borrow().is_none() {
            writer.write_all(&self.dir_name_hash)?;
        }

        for subfolder in self.subfolders.iter() {
            subfolder.serialize_subdirs(writer)?;
        }

        if direct_files_count > 0 {
            writer.write_all(&self.content_hash)?;
        }

        Ok(())
    }

    /// Remove subdir names that are too long or unnecessary
    fn prune_subdir_names(&self) -> Result<PackingInfo, Box<dyn Error>> {
        let direct_files_count = self.total_files_count - self.subfolders.iter().fold(0, |sum, e|
            sum + e.total_files_count
        );

        let mut need_folder_full_path = direct_files_count == 0;
        for subfolder in self.subfolders.iter() {
            let sub_pack_info = subfolder.prune_subdir_names()?;
            need_folder_full_path |= sub_pack_info.need_folder_full_path;
        }
        if self.total_files_count > 0 {
            // Files know their path, so we only need to keep the folder name if we don't have any
            need_folder_full_path = false;
        }

        // We store the name instead of the hash if it's short enough, or if we genuinely need it
        // We keep names up to 2x the hash size since they typically compress very well
        let mut dir_name = self.dir_name.borrow_mut();
        if need_folder_full_path {
            dir_name.as_ref().expect("Cannot serialize DirStat without dir names");
        } else if dir_name.is_some() && dir_name.as_ref().unwrap().len() > 16 {
            *dir_name = None;
        }

        Ok(PackingInfo {
            need_folder_full_path,
        })
    }

    fn serialize_numeric_bitstream<F, W>(&self, bitstream_writer: &mut BitstreamWriter<W>, get_number: &F) -> Result<(), Box<dyn Error>>
        where F: Fn(&DirStat) -> u64, W: Write {
        bitstream_writer.write(get_number(self))?;

        for subfolder in self.subfolders.iter() {
            subfolder.serialize_numeric_bitstream(bitstream_writer, get_number)?;
        }

        Ok(())
    }

    /// Serialized the directory stats into a writer.
    /// On error partial data may have been written.
    pub fn serialize_into<W: Write>(&self, writer: &mut W) -> Result<(), Box<dyn Error>> {
        self.prune_subdir_names()?;
        let encoding_settings = best_encoding_settings(&self);

        {
            let mut file_count_bitstream_writer = BitstreamWriter::new(writer, encoding_settings.file_counts);
            self.serialize_numeric_bitstream(&mut file_count_bitstream_writer, &|stat| {
                stat.total_files_count - stat.subfolders.iter().fold(0, |sum, e|
                    sum + e.total_files_count
                )
            })?;
        }

        {
            let mut folder_count_bitstream_writer = BitstreamWriter::new(writer, encoding_settings.subdirs_counts);
            self.serialize_numeric_bitstream(&mut folder_count_bitstream_writer, &|stat| stat.subfolders.len() as u64)?;
        }

        {
            let mut dirname_len_bitstream_writer = BitstreamWriter::new(writer, encoding_settings.dirname_counts);
            self.serialize_numeric_bitstream(&mut dirname_len_bitstream_writer, &|stat| {
                match stat.dir_name.borrow().as_ref() {
                    Some(name) => name.len() as u64,
                    None => 0,
                }
            })?;
        }

        let mut dirnames_buf = Vec::new();
        let mut compressor = Encoder::new(&mut dirnames_buf, 22)?;
        self.serialize_dirnames(&mut compressor)?;
        compressor.finish()?;
        leb128::write::unsigned(writer, dirnames_buf.len() as u64)?;
        writer.write_all(&dirnames_buf)?;

        self.serialize_subdirs(writer)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::path::Path;
    use crate::dirdb::DirStat;

    #[test]
    fn serialize_roundtrip() -> Result<(), Box<dyn Error>> {
        let stat = DirStat::new(Path::new("test_data"))?;
        let mut serialized = Vec::new();
        stat.serialize_into(&mut serialized)?;

        let unserialized = DirStat::new_from_bytes(&mut &serialized[..])?;
        assert_eq!(stat, unserialized);

        let mut reserialized = Vec::new();
        unserialized.serialize_into(&mut reserialized)?;
        assert_eq!(serialized, reserialized);
        Ok(())
    }
}
