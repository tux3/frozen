use super::*;

pub struct BitstreamReader<'r> {
    data: &'r [u8],
    pos: usize,
    encoding: Encoding,
}

impl<'r> BitstreamReader<'r> {
    pub fn new(mut data: &'r [u8]) -> Self {
        let encoded_data_size = leb128::read::unsigned(&mut data).unwrap() as usize;

        let header = data[0] >> (8 - ENCODING_SIGNALING_OVERHEAD);
        let use_varint = header >> ENCODING_BITS_BITS == 1;
        let bits = (header & ((1 << ENCODING_BITS_BITS) - 1)) as usize;

        let encoding = Encoding {
            use_varint,
            bits,
            encoded_data_size,
        };

        Self {
            data,
            pos: ENCODING_SIGNALING_OVERHEAD,
            encoding,
        }
    }

    fn read_bits(&mut self, count: usize) -> u64 {
        let mut remaining = count;
        let mut result = 0u64;
        if self.pos % 8 != 0 && remaining > 8 - self.pos % 8 {
            let to_read = 8 - self.pos % 8;
            result = u64::from(self.data[self.pos / 8] & ((1 << to_read) - 1));
            self.pos += to_read;
            remaining -= to_read;
        }

        while remaining >= 8 {
            result <<= 8;
            result |= u64::from(self.data[self.pos / 8]);
            remaining -= 8;
            self.pos += 8;
        }

        if remaining != 0 {
            let remaining_bitmask = (1 << remaining) - 1;
            let discarded_bits = 8 - (self.pos % 8) - remaining;
            let read_value = u64::from(self.data[self.pos / 8]) >> discarded_bits & remaining_bitmask;
            result = (result << remaining) | read_value;
            self.pos += remaining;
        }

        result
    }

    pub fn read(&mut self) -> u64 {
        if !self.encoding.use_varint {
            return self.read_bits(self.encoding.bits);
        }

        let cont_flag = 1 << (self.encoding.bits - 1);
        let mut cur_shift = 0;
        let mut result = 0u64;
        loop {
            let elem = self.read_bits(self.encoding.bits);
            result |= (elem & !cont_flag) << cur_shift;
            if elem & cont_flag == 0 {
                return result;
            }
            cur_shift += self.encoding.bits - 1
        }
    }

    pub fn slice_after(&self) -> &'r [u8] {
        let total_bits = self.encoding.encoded_data_size;
        let total_bytes = total_bits / 8 + (total_bits % 8 != 0) as usize;
        &self.data[total_bytes..]
    }
}

#[cfg(test)]
mod tests {
    use super::super::Encoding;
    use super::BitstreamReader;
    use std::error::Error;

    #[test]
    fn read_bits_by_8() -> Result<(), Box<dyn Error>> {
        let to_read = [0u8, 1, 17, 42, 254, 255];
        let mut stream = BitstreamReader {
            data: &to_read,
            pos: 0,
            encoding: Encoding {
                use_varint: false,
                bits: 8,
                encoded_data_size: 0,
            },
        };
        for &byte in to_read.iter() {
            assert_eq!(stream.read_bits(8), byte as u64);
        }

        Ok(())
    }

    #[test]
    fn read_bits_by_5() -> Result<(), Box<dyn Error>> {
        let elems = [5u32, 16, 31, 11, 0, 7];
        let concat: u32 = elems.iter().fold(0, |sum, e| (sum << 5) | e) << 2;
        let to_read = concat.to_be_bytes();
        let mut stream = BitstreamReader {
            data: &to_read,
            pos: 0,
            encoding: Encoding {
                use_varint: false,
                bits: 8,
                encoded_data_size: 0,
            },
        };
        for &elem in elems.iter() {
            assert_eq!(stream.read_bits(5), elem as u64);
        }

        Ok(())
    }

    #[test]
    fn read_bits_large_unaligned() -> Result<(), Box<dyn Error>> {
        let concat: u32 = (0b111 << 29) | (0xAABBCC << 5) | (0b10001);
        let to_read = concat.to_be_bytes();
        let mut stream = BitstreamReader {
            data: &to_read,
            pos: 0,
            encoding: Encoding {
                use_varint: false,
                bits: 8,
                encoded_data_size: 0,
            },
        };
        assert_eq!(stream.read_bits(3), 0b111);
        assert_eq!(stream.read_bits(24), 0xAABBCC);
        assert_eq!(stream.read_bits(4), 0b1000);
        assert_eq!(stream.read_bits(1), 0b1);

        Ok(())
    }
}
