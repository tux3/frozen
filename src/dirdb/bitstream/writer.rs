use super::*;
use eyre::{ensure, Result};
use std::io::Write;

pub struct BitstreamWriter<'w, W: Write> {
    writer: &'w mut W,
    encoding: Encoding,
    buf: u16, // We only need 8 bits, but we want to be able to shift left by 8
    buf_used: usize,
    written: usize,
    finished: bool,
}

impl<'w, W: Write> BitstreamWriter<'w, W> {
    pub fn new(writer: &'w mut W, encoding: Encoding) -> Self {
        leb128::write::unsigned(writer, encoding.encoded_data_size as u64).unwrap();

        assert!(encoding.bits < 1 << ENCODING_BITS_BITS);
        let encoding_header = ((encoding.use_varint as u64) << ENCODING_BITS_BITS) | encoding.bits as u64;

        let mut stream = Self {
            writer,
            encoding,
            buf: 0,
            buf_used: 0,
            written: 0,
            finished: false,
        };

        stream.write_bits(encoding_header, ENCODING_SIGNALING_OVERHEAD).unwrap();
        stream
    }

    fn write_bits(&mut self, mut bits: u64, mut size: usize) -> Result<()> {
        ensure!(!self.finished, "Cannot write to a bitstream after calling finish()");
        self.written += size;

        let mut remaining_buf_bits = 8 - self.buf_used;
        while size > remaining_buf_bits {
            let val = [(self.buf << remaining_buf_bits) as u8 | (bits >> (size - remaining_buf_bits)) as u8];
            self.writer.write_all(&val)?;
            self.buf_used = 0;
            size -= remaining_buf_bits;
            bits &= (1 << size) - 1;
            remaining_buf_bits = 8;
        }

        self.buf = (self.buf << size) | bits as u16;
        self.buf_used += size;

        if self.buf_used == 8 {
            let val = [self.buf as u8];
            self.writer.write_all(&val[..])?;
            self.buf_used = 0;
        }

        Ok(())
    }

    pub fn write(&mut self, item: u64) -> Result<()> {
        if self.encoding.bits == 0 {
            return Ok(()); // I mean sure, why not encode an empty bitstream!
        }

        let encoding_data_bits = self.encoding.bits - self.encoding.use_varint as usize;
        let item_bits = (f64::log2((item + 1) as f64).ceil() as usize).max(1);
        let elems_needed = item_bits / encoding_data_bits + (item_bits % encoding_data_bits != 0) as usize;

        if !self.encoding.use_varint {
            assert!(item_bits <= encoding_data_bits);
            return self.write_bits(item, encoding_data_bits);
        }

        let mut remaining_data = item;
        for _ in 0..elems_needed - 1 {
            let continuation_bit = 1 << encoding_data_bits;
            let elem_data = remaining_data & ((1 << encoding_data_bits) - 1);
            let encoded = continuation_bit | elem_data;
            self.write_bits(encoded, self.encoding.bits)?;
            remaining_data >>= encoding_data_bits;
        }

        self.write_bits(remaining_data, self.encoding.bits)?;

        Ok(())
    }

    pub fn finish(&mut self) {
        assert!(!self.finished);
        assert_eq!(self.encoding.encoded_data_size, self.written);
        if self.buf_used == 0 {
            return;
        }
        self.buf <<= 8 - self.buf_used;
        let val = [self.buf as u8];
        self.writer.write_all(&val).unwrap();
        self.finished = true;
    }
}

impl<'w, W: Write> Drop for BitstreamWriter<'w, W> {
    fn drop(&mut self) {
        if !self.finished {
            self.finish();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::Encoding;
    use super::BitstreamWriter;
    use eyre::Result;

    #[test]
    fn write_raw_bytes() -> Result<()> {
        let to_encode = [0u8, 1, 17, 42, 254, 255];
        let mut writer = Vec::new();
        let mut stream = BitstreamWriter {
            writer: &mut writer,
            encoding: Encoding {
                use_varint: false,
                bits: 8,
                encoded_data_size: to_encode.len() * 8,
            },
            buf: 0,
            buf_used: 0,
            written: 0,
            finished: false,
        };
        for &byte in to_encode.iter() {
            stream.write(byte as u64)?;
        }
        drop(stream);

        assert_eq!(writer.as_ref(), to_encode);
        Ok(())
    }

    #[test]
    fn write_raw_nibbles() -> Result<()> {
        let to_encode = [0u8, 1, 17, 42, 254, 255];
        let mut writer = Vec::new();
        let mut stream = BitstreamWriter {
            writer: &mut writer,
            encoding: Encoding {
                use_varint: false,
                bits: 4,
                encoded_data_size: to_encode.len() * 8,
            },
            buf: 0,
            buf_used: 0,
            written: 0,
            finished: false,
        };
        for &byte in to_encode.iter() {
            stream.write((byte >> 4) as u64)?;
            stream.write((byte & 0xF) as u64)?;
        }
        drop(stream);

        assert_eq!(writer.as_ref(), to_encode);
        Ok(())
    }

    #[test]
    #[allow(clippy::identity_op)] // Come on! Code is for humans, not linters!
    fn write_leb128() -> Result<()> {
        let to_encode = [0, 1, 17, 42, 127, 128, 254, 255, 25519, std::u64::MAX - 1];
        let mut writer = Vec::new();
        let mut stream = BitstreamWriter {
            writer: &mut writer,
            encoding: Encoding {
                use_varint: true,
                bits: 8,
                encoded_data_size: 5 * 8 * 1 + 3 * 8 * 2 + 1 * 8 * 3 + 1 * 8 * 10,
            },
            buf: 0,
            buf_used: 0,
            written: 0,
            finished: false,
        };
        for &byte in to_encode.iter() {
            stream.write(byte)?;
        }
        drop(stream);

        let mut reader = writer.as_slice();
        for &byte in to_encode.iter() {
            assert_eq!(leb128::read::unsigned(&mut reader)?, byte);
        }

        Ok(())
    }
}
