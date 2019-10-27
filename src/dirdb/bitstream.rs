mod reader;
mod writer;

///! A simple integer bitstream reader and writer.
///! It is required to know the encoding (including the encoded size of the data) before encoding

pub use reader::BitstreamReader;
pub use writer::BitstreamWriter;

// 1 bit for the raw/varint flag
pub const ENCODING_FLAGS_BITS: usize = 1;
// We want encoding signaling to be constant size, so we can't signal arbitrarily large sizes
// This means we can have at most 2**encoding_bits_size wide encodings
pub const ENCODING_BITS_BITS: usize = 4;
pub const ENCODING_SIGNALING_OVERHEAD: usize = ENCODING_FLAGS_BITS + ENCODING_BITS_BITS;

/// The encoding for a data stream of a particular size
pub struct Encoding {
    /// Use a varint instead of a raw fixed-size encoding
    pub use_varint: bool,
    /// Size in bits of each element. For varints, includes the continuation flag overhead.
    pub bits: usize,
    /// Size in bits of the data stream to be serialized with this encoding (not counting signaling)
    pub encoded_data_size: usize,
}


#[cfg(test)]
mod tests {
    use std::error::Error;
    use super::*;
    use crate::box_result::BoxResult;

    #[test]
    fn roundtrip_raw_bytes() -> BoxResult<()> {
        let to_encode = [0u8, 1, 17, 42, 254, 255];
        let mut buf = Vec::new();
        let mut wstream = BitstreamWriter::new(&mut buf, Encoding {
            use_varint: false,
            bits: 8,
            encoded_data_size: to_encode.len() * 8 + ENCODING_SIGNALING_OVERHEAD,
        });
        for &byte in to_encode.iter() {
            wstream.write(byte as u64)?;
        }
        drop(wstream);

        let mut rstream = BitstreamReader::new(&buf);
        for &byte in to_encode.iter() {
            assert_eq!(byte as u64, rstream.read());
        }

        Ok(())
    }

    #[test]
    fn roundtrip_raw_31_bits() -> BoxResult<()> {
        let to_encode = [0u64, 1, 17, 42, 254, 255, 25519, (std::u16::MAX/2) as u64];
        let mut buf = Vec::new();
        let mut wstream = BitstreamWriter::new(&mut buf, Encoding {
            use_varint: false,
            bits: 15,
            encoded_data_size: to_encode.len() * 15 + ENCODING_SIGNALING_OVERHEAD,
        });
        for &val in to_encode.iter() {
            wstream.write(val)?;
        }
        drop(wstream);

        let mut rstream = BitstreamReader::new(&buf);
        for &val in to_encode.iter() {
            assert_eq!(val, rstream.read());
        }

        Ok(())
    }

    #[test]
    fn roundtrip_vuint_14_bits() -> BoxResult<()> {
        let to_encode = [0u64, 1, 17, 42, 254, 255, std::u32::MAX as u64];
        let mut buf = Vec::new();
        let mut wstream = BitstreamWriter::new(&mut buf, Encoding {
            use_varint: true,
            bits: 14,
            encoded_data_size: (to_encode.len() + 2)*14 + ENCODING_SIGNALING_OVERHEAD,
        });
        for &val in to_encode.iter() {
            wstream.write(val)?;
        }
        drop(wstream);

        let mut rstream = BitstreamReader::new(&buf);
        for &val in to_encode.iter() {
            assert_eq!(val as u64, rstream.read());
        }

        Ok(())
    }

    #[test]
    fn roundtrip_vuint_7_bits() -> BoxResult<()> {
        let to_encode = [0u8, 1, 17, 42, 254, 255];
        let mut buf = Vec::new();
        let mut wstream = BitstreamWriter::new(&mut buf, Encoding {
            use_varint: true,
            bits: 7,
            encoded_data_size: (to_encode.len() + 2) * 7 + ENCODING_SIGNALING_OVERHEAD,
        });
        for &byte in to_encode.iter() {
            wstream.write(byte as u64)?;
        }
        drop(wstream);

        let mut rstream = BitstreamReader::new(&buf);
        for &byte in to_encode.iter() {
            let val = rstream.read();
            assert_eq!(byte as u64, val);
        }

        Ok(())
    }

    #[test]
    fn roundtrip_vuint_2_bits() -> BoxResult<()> {
        let to_encode = [0u8, 1, 17, 42, 254, 255];
        let mut buf = Vec::new();
        let mut wstream = BitstreamWriter::new(&mut buf, Encoding {
            use_varint: true,
            bits: 2,
            encoded_data_size: 58 + ENCODING_SIGNALING_OVERHEAD,
        });
        for &byte in to_encode.iter() {
            wstream.write(byte as u64)?;
        }
        drop(wstream);

        let mut rstream = BitstreamReader::new(&buf);
        for &byte in to_encode.iter() {
            let val = rstream.read();
            assert_eq!(byte as u64, val);
        }

        Ok(())
    }
}