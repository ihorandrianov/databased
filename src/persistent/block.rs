use core::str;
use std::iter;

use super::errors::PersistentLayerError;

const PAGE_SIZE: u16 = 8192;
const HEADER_SIZE: usize = 12;
const LP_LEN: usize = 32;
const IDX_LEN: usize = 2;

struct LinePointer<const MAX_LEN: usize>(String, u16);

impl<const MAX_LEN: usize> LinePointer<MAX_LEN> {
    pub fn new(value: &str, idx: u16) -> Result<Self, PersistentLayerError> {
        let key_byte_len = value.as_bytes().len();
        if key_byte_len > MAX_LEN {
            return Err(PersistentLayerError::LinePointerLenError(
                MAX_LEN,
                key_byte_len,
            ));
        }

        Ok(Self(value.to_string(), idx))
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes: Vec<u8> = self.0.bytes().collect();
        let diff = MAX_LEN - bytes.len();

        bytes.extend(iter::repeat(0).take(diff));
        bytes.extend(self.1.to_le_bytes());
        bytes
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, PersistentLayerError> {
        if bytes.len() != MAX_LEN + IDX_LEN {
            return Err(PersistentLayerError::LinePointerSerializationError);
        };

        let key_bytes = &bytes[..MAX_LEN];
        let key = str::from_utf8(&key_bytes)?;

        let key = key.trim_end_matches(char::from(0));

        let idx = &bytes[MAX_LEN..];
        let idx = u16::from_le_bytes([idx[0], idx[1]]);

        Ok(Self(key.to_string(), idx))
    }

    pub fn get_byte_size() -> usize {
        MAX_LEN + IDX_LEN
    }
}

struct ValueEntry {
    len: u32,
    value: String,
}

impl ValueEntry {
    fn from_str(value: &str) -> Result<Self, PersistentLayerError> {
        let len: u32 = value
            .len()
            .try_into()
            .map_err(|_| PersistentLayerError::ValueTooLong)?;

        Ok(Self {
            len,
            value: value.to_string(),
        })
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut result = self.len.to_le_bytes().to_vec();

        result.extend(self.value.bytes());

        result
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, PersistentLayerError> {
        if bytes.len() < 5 {
            return Err(PersistentLayerError::ValueEntryOOBError);
        }
        let len = &bytes[..4];
        let len = u32::from_le_bytes([len[0], len[1], len[2], len[3]]);
        let key = &bytes[4..];
        let key = str::from_utf8(&key)?;

        Ok(Self {
            len,
            value: key.to_string(),
        })
    }
}

struct Block {
    header: Header,
    data: Vec<u8>,
}

impl Block {
    fn construct(
        init_data: Vec<(String, String)>,
    ) -> Result<(Option<Self>, Vec<(String, String)>), PersistentLayerError> {
        let mut header = Header::default();
        let mut byte_block: Vec<u8> = vec![0u8; PAGE_SIZE as usize];
        let mut not_fit_records: Vec<(String, String)> = vec![];
        let header_offset: u16 = HEADER_SIZE
            .try_into()
            .expect("Header size should be in bounds of u16");
        let upper_cursor: u16 = byte_block.len() as u16 - 1;
        let mut write_flag = false;
        header.set(HeaderProps::LINP(header_offset));
        header.set(HeaderProps::LOWER(header_offset));
        header.set(HeaderProps::UPPER(upper_cursor));
        for (key, value) in init_data.into_iter() {
            let result = Self::write_record(&key, &value, &mut byte_block, &mut header);
            if let Err(_) = result {
                not_fit_records.push((key, value));
            } else {
                if !write_flag {
                    write_flag = true;
                }
            }
        }

        let mut block = Self {
            header,
            data: byte_block,
        };

        if write_flag {
            let checksum = block.calculate_fletcher16();
            block.header.set(HeaderProps::CHECKSUM(checksum));
            Ok((Some(block), not_fit_records))
        } else {
            Ok((None, not_fit_records))
        }
    }

    fn write_record(
        key: &str,
        value: &str,
        byte_block: &mut Vec<u8>,
        header: &mut Header,
    ) -> Result<(), PersistentLayerError> {
        let value = ValueEntry::from_str(value)?;
        let value_bytes = value.to_bytes();
        let upper = header.upper();
        let lower = header.lower();
        let new_upper = (upper as usize)
            .checked_sub(value_bytes.len())
            .ok_or(PersistentLayerError::NoSpaceInBlock)?;

        let lp = LinePointer::<LP_LEN>::new(key, new_upper as u16)?;
        let lp_bytes = lp.to_bytes();
        let new_lower = lower as usize + lp_bytes.len();
        if !header.is_space_to_write(lp_bytes.len() + value_bytes.len()) {
            return Err(PersistentLayerError::NoSpaceInBlock);
        }
        byte_block[new_upper..upper as usize].copy_from_slice(&value_bytes);
        byte_block[lower as usize..new_lower].copy_from_slice(&lp_bytes);
        header.set(HeaderProps::LOWER(new_lower as u16));
        header.set(HeaderProps::UPPER(new_upper as u16));
        return Ok(());
    }

    fn to_bytes(&self) -> Vec<u8> {
        let mut result = Vec::with_capacity(PAGE_SIZE as usize);
        result.extend(self.header.to_bytes());
        result.extend(&self.data);
        result
    }

    fn from_bytes(data: &[u8]) -> Result<Self, PersistentLayerError> {
        let header = Header::from_bytes(&data[..HEADER_SIZE])?;
        let data = &data[HEADER_SIZE..];

        let block = Self {
            header,
            data: data.to_vec(),
        };

        if block.validate_checksum() {
            Ok(block)
        } else {
            Err(PersistentLayerError::ChecksumValidationError)
        }
    }

    fn calculate_fletcher16(&self) -> u16 {
        let data = &self.data;
        let mut sum1: u16 = 0;
        let mut sum2: u16 = 0;

        for &byte in data {
            sum1 = (sum1 + byte as u16) % 255;
            sum2 = (sum2 + sum1) % 255;
        }

        (sum2 << 8) | sum1
    }

    fn validate_checksum(&self) -> bool {
        let control = self.calculate_fletcher16();
        let checksum = self.header.checksum();
        checksum == control
    }
}

struct Header {
    checksum: u16,
    flags: u16,
    lower: u16,
    upper: u16,
    linp: u16,
    page_size: u16,
}

impl Default for Header {
    fn default() -> Self {
        Self {
            page_size: PAGE_SIZE,
            checksum: 0,
            flags: 0,
            lower: 0,
            upper: 0,
            linp: 0,
        }
    }
}

impl Header {
    fn from_bytes(bytes: &[u8]) -> Result<Self, PersistentLayerError> {
        if bytes.len() != HEADER_SIZE {
            return Err(PersistentLayerError::HeaderError(
                "Header serialization error, header is too short".to_string(),
            ));
        };

        let mut chunks = bytes.chunks_exact(2);

        let page_size = u16::from_le_bytes(
            chunks
                .next()
                .ok_or_else(|| {
                    PersistentLayerError::HeaderError("Failed to extract page_size".to_string())
                })?
                .try_into()
                .map_err(|_| PersistentLayerError::HeaderError("Conversion error".to_string()))?,
        );

        let checksum = u16::from_le_bytes(
            chunks
                .next()
                .ok_or_else(|| {
                    PersistentLayerError::HeaderError("Failed to extract checksum".to_string())
                })?
                .try_into()
                .map_err(|_| PersistentLayerError::HeaderError("Conversion error".to_string()))?,
        );

        let flags = u16::from_le_bytes(
            chunks
                .next()
                .ok_or_else(|| {
                    PersistentLayerError::HeaderError("Failed to extract flags".to_string())
                })?
                .try_into()
                .map_err(|_| PersistentLayerError::HeaderError("Conversion error".to_string()))?,
        );

        let lower = u16::from_le_bytes(
            chunks
                .next()
                .ok_or_else(|| {
                    PersistentLayerError::HeaderError("Failed to extract lower".to_string())
                })?
                .try_into()
                .map_err(|_| PersistentLayerError::HeaderError("Conversion error".to_string()))?,
        );

        let upper = u16::from_le_bytes(
            chunks
                .next()
                .ok_or_else(|| {
                    PersistentLayerError::HeaderError("Failed to extract upper".to_string())
                })?
                .try_into()
                .map_err(|_| PersistentLayerError::HeaderError("Conversion error".to_string()))?,
        );

        let linp = u16::from_le_bytes(
            chunks
                .next()
                .ok_or_else(|| {
                    PersistentLayerError::HeaderError("Failed to extract linp".to_string())
                })?
                .try_into()
                .map_err(|_| PersistentLayerError::HeaderError("Conversion error".to_string()))?,
        );

        Ok(Self {
            page_size,
            checksum,
            flags,
            lower,
            upper,
            linp,
        })
    }

    fn to_bytes(&self) -> Vec<u8> {
        let mut result = Vec::with_capacity(HEADER_SIZE);
        result.extend(self.page_size.to_le_bytes());
        result.extend(self.checksum.to_le_bytes());
        result.extend(self.flags.to_le_bytes());
        result.extend(self.lower.to_le_bytes());
        result.extend(self.upper.to_le_bytes());
        result.extend(self.linp.to_le_bytes());
        result
    }

    fn set(&mut self, value: HeaderProps) {
        match value {
            HeaderProps::CHECKSUM(v) => self.checksum = v,
            HeaderProps::FLAGS(v) => self.flags = v,
            HeaderProps::LINP(v) => self.linp = v,
            HeaderProps::LOWER(v) => self.lower = v,
            HeaderProps::UPPER(v) => self.upper = v,
        };
    }

    fn upper(&self) -> u16 {
        self.upper
    }

    fn lower(&self) -> u16 {
        self.lower
    }

    fn checksum(&self) -> u16 {
        self.checksum
    }

    fn is_space_to_write(&self, size: usize) -> bool {
        let diff = (self.upper - self.lower) as usize;
        if diff < size {
            false
        } else {
            true
        }
    }
}

enum HeaderProps {
    CHECKSUM(u16),
    FLAGS(u16),
    LOWER(u16),
    UPPER(u16),
    LINP(u16),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_line_pointer_new_success() {
        let result = LinePointer::<LP_LEN>::new("key", 100);
        assert!(result.is_ok());

        let line_ptr = result.unwrap();
        assert_eq!(line_ptr.0, "key");
        assert_eq!(line_ptr.1, 100);
    }

    #[test]
    fn test_line_pointer_new_failure() {
        let long_key = "a".repeat(33); // MAX_LEN is 32, so this should fail
        let result = LinePointer::<LP_LEN>::new(&long_key, 100);
        assert!(result.is_err());
    }

    #[test]
    fn test_line_pointer_to_bytes() {
        let line_ptr = LinePointer::<LP_LEN>::new("key", 100).unwrap();
        let bytes = line_ptr.to_bytes();

        // The first part should contain the key, zero-padded up to 32 bytes
        assert_eq!(&bytes[0..3], b"key");
        assert_eq!(&bytes[3..32], &[0u8; 29]);

        // The last two bytes should be the index in little-endian format
        assert_eq!(u16::from_le_bytes([bytes[32], bytes[33]]), 100);
    }

    #[test]
    fn test_line_pointer_from_bytes() {
        let line_ptr = LinePointer::<LP_LEN>::new("key", 100).unwrap();
        let bytes = line_ptr.to_bytes();

        let decoded = LinePointer::<LP_LEN>::from_bytes(&bytes).unwrap();
        assert_eq!(decoded.0, "key");
        assert_eq!(decoded.1, 100);
    }

    #[test]
    fn test_line_pointer_from_bytes_failure() {
        let invalid_bytes = vec![0u8; 31]; // Not the correct size (needs 34)
        let result = LinePointer::<LP_LEN>::from_bytes(&invalid_bytes);
        assert!(result.is_err());
    }
}

#[cfg(test)]
mod value_entry_tests {
    use super::*;

    #[test]
    fn test_value_entry_from_str() {
        let value_entry = ValueEntry::from_str("value").unwrap();
        assert_eq!(value_entry.len, 5);
        assert_eq!(value_entry.value, "value");
    }

    #[test]
    fn test_value_entry_to_bytes() {
        let value_entry = ValueEntry::from_str("value").unwrap();
        let bytes = value_entry.to_bytes();

        // First 4 bytes should represent the length (5 in this case)
        assert_eq!(
            u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]),
            5
        );

        // Remaining bytes should represent the string "value"
        assert_eq!(&bytes[4..], b"value");
    }

    #[test]
    fn test_value_entry_from_bytes() {
        let value_entry = ValueEntry::from_str("value").unwrap();
        let bytes = value_entry.to_bytes();

        let decoded = ValueEntry::from_bytes(&bytes).unwrap();
        assert_eq!(decoded.len, 5);
        assert_eq!(decoded.value, "value");
    }

    #[test]
    fn test_value_entry_from_bytes_failure() {
        let invalid_bytes = vec![0u8; 3]; // Too short to be valid
        let result = ValueEntry::from_bytes(&invalid_bytes);
        assert!(result.is_err());
    }
}

#[cfg(test)]
mod block_tests {
    use super::*;

    #[test]
    fn test_block_construct_success() {
        let init_data = vec![
            ("key1".to_string(), "value1".to_string()),
            ("key2".to_string(), "value2".to_string()),
        ];

        let (block, not_fit) = Block::construct(init_data).unwrap();
        let block = block.unwrap();

        // Check that no records failed to fit in the block
        assert!(not_fit.is_empty());

        // Ensure checksum was calculated
        assert!(block.header.checksum != 0);
    }

    #[test]
    fn test_block_construct_with_overflow() {
        let init_data = vec![
            ("key1".to_string(), "value1".to_string()),
            ("key2".to_string(), "value2".to_string()),
            // Adding an extremely large value to force overflow
            ("key3".to_string(), "a".repeat(8200)),
        ];

        let (block, not_fit) = Block::construct(init_data).unwrap();

        // Ensure that some records didn't fit
        assert!(!not_fit.is_empty());
    }

    #[test]
    fn test_block_to_bytes_and_from_bytes() {
        let init_data = vec![
            ("key1".to_string(), "value1".to_string()),
            ("key2".to_string(), "value2".to_string()),
        ];

        let (block, _) = Block::construct(init_data).unwrap();
        let block = block.unwrap();
        let bytes = &block.to_bytes();

        let reconstructed_block = Block::from_bytes(&bytes).unwrap();

        assert_eq!(reconstructed_block.data.len(), block.data.len());
        assert_eq!(reconstructed_block.header.checksum, block.header.checksum);
    }
}

#[cfg(test)]
mod header_tests {
    use super::*;

    #[test]
    fn test_header_default() {
        let header = Header::default();
        assert_eq!(header.page_size, PAGE_SIZE);
        assert_eq!(header.checksum, 0);
        assert_eq!(header.flags, 0);
        assert_eq!(header.lower, 0);
        assert_eq!(header.upper, 0);
        assert_eq!(header.linp, 0);
    }

    #[test]
    fn test_header_to_and_from_bytes() {
        let mut header = Header::default();
        header.set(HeaderProps::CHECKSUM(12345));
        header.set(HeaderProps::LOWER(100));
        header.set(HeaderProps::UPPER(200));

        let bytes = header.to_bytes();
        let decoded_header = Header::from_bytes(&bytes).unwrap();

        assert_eq!(decoded_header.checksum, 12345);
        assert_eq!(decoded_header.lower, 100);
        assert_eq!(decoded_header.upper, 200);
    }

    #[test]
    fn test_header_from_bytes_failure() {
        let invalid_bytes = vec![0u8; 10]; // Incorrect size
        let result = Header::from_bytes(&invalid_bytes);
        assert!(result.is_err());
    }
}
