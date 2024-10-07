use std::iter;

use super::errors::PersistentLayerError;

const PAGE_SIZE: u16 = 8192;
const HEADER_SIZE: usize = 12;

struct LinePointer<const MAX_LEN: usize>(String, u16);

impl<const MAX_LEN: usize> LinePointer<MAX_LEN> {
    fn new(value: String, idx: u16) -> Result<Self, PersistentLayerError> {
        let key_byte_len = value.as_bytes().len();
        if key_byte_len > MAX_LEN {
            return Err(PersistentLayerError::LinePointerLenError(
                MAX_LEN,
                key_byte_len,
            ));
        }

        Ok(Self(value, idx))
    }

    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes: Vec<u8> = self.0.bytes().collect();
        let diff = MAX_LEN - bytes.len();

        bytes.extend(iter::repeat(0).take(diff));
        bytes.extend(self.1.to_le_bytes());
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, PersistentLayerError> {
        if bytes.len() != MAX_LEN + 2 {
            return Err(PersistentLayerError::LinePointerSerializationError);
        };

        let key_bytes = &bytes[..MAX_LEN];
        let key = String::from_utf8(key_bytes.to_vec())?;
        let key = key.trim_end_matches(char::from(0));

        let idx = &bytes[MAX_LEN..];
        let idx = u16::from_le_bytes([idx[0], idx[1]]);

        Ok(Self(key.to_string(), idx))
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
        let len = &bytes[..4];
        let len = u32::from_le_bytes([len[0], len[1], len[2], len[3]]);
        let key = &bytes[4..4 + len as usize];
        let key = String::from_utf8(key.to_vec())?;

        Ok(Self { len, value: key })
    }
}

struct Block {
    header: Header,
    data: Vec<u8>,
}

impl Block {
    fn construct(
        init_data: Vec<(String, String)>,
    ) -> Result<(Self, Vec<(String, String)>), PersistentLayerError> {
        let mut header = Header::default();
        let mut byte_block: Vec<u8> = vec![0u8; PAGE_SIZE as usize];
        let mut not_fit_records: Vec<(String, String)> = vec![];
        let header_offset: u16 = HEADER_SIZE
            .try_into()
            .expect("Header size should be in bounds of u16");
        let upper_cursor: u16 = byte_block.len() as u16 - 1;
        header.set(HeaderProps::LINP(header_offset));
        header.set(HeaderProps::LOWER(header_offset));
        header.set(HeaderProps::UPPER(upper_cursor));
        for (key, value) in init_data.iter() {
            let value = ValueEntry::from_str(value)?;
            let value_bytes = value.to_bytes();
            let upper = header.upper();
            let lower = header.lower();
            let new_upper = upper as usize - value_bytes.len();
            let lp = LinePointer::<32>::new(key.to_string(), new_upper as u16)?;
            let lp_bytes = lp.to_bytes();
            let new_lower = lower as usize + lp_bytes.len();
            if header.is_space_to_write(lp_bytes.len() + value_bytes.len()) {
                byte_block[new_upper..upper as usize].copy_from_slice(&value_bytes);
                byte_block[lower as usize..new_lower].copy_from_slice(&lp_bytes);
                header.set(HeaderProps::LOWER(new_lower as u16));
                header.set(HeaderProps::UPPER(new_upper as u16));
            } else {
                not_fit_records.push((key.to_string(), value.value))
            }
        }

        Ok((
            Self {
                header,
                data: byte_block,
            },
            not_fit_records,
        ))
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

    fn is_space_to_write(&self, size: usize) -> bool {
        let diff = (self.lower - self.upper) as usize;
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
