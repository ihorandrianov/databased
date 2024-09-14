use crate::{errors::BytecodeSerializerError, operation::Op};
use lazy_static::lazy_static;

/*
TODO LIST
Summary of Suggestions:
Refactor CRC logic to avoid repetition.
Abstract common parts of the serialization process (e.g., header creation, timestamp/key serialization).
Add edge case handling for out-of-bound access and invalid input lengths.
Ensure zero handling is consistent in convert_to_varint and deserialization.
Prevent overflows in convert_from_varint and handle large shifts.
Optimize CRC computation and avoid unnecessary memory copies.
Return a Result from op_to_bytes to allow error handling during serialization.
Provide more detailed error messages for debugging.
*/

const START_MAGIC: u32 = 0xDEFEC8ED;
const END_MAGIC: u32 = 0xB00B1E5;
const PROTOCOL_VERSION: u8 = 0b0000_0000;
const PROTOCOL_BITMASK: u8 = 0b1111_0000;
const OPERATION_BITMASK: u8 = 0b0000_1111;
const SET_OPERATION: u8 = 0b0000_0001;
const GET_OPERATION: u8 = 0b0000_0010;
const DEL_OPERATION: u8 = 0b0000_0100;
const CRC_POLYNOMIAL: u32 = 0xedb88320;
lazy_static! {
    static ref START_MAGIC_BYTES: [u8; 4] = START_MAGIC.to_le_bytes();
    static ref END_MAGIC_BYTES: [u8; 4] = END_MAGIC.to_le_bytes();
    static ref LOOKUP_CRC_TABLE: Vec<u32> = {
        let mut table = vec![0; 256];
        for i in 0..256 {
            let mut crc = i as u32;
            for _ in 0..8 {
                if crc & 1 == 1 {
                    crc = CRC_POLYNOMIAL ^ (crc >> 1);
                } else {
                    crc >>= 1;
                }
            }
            table[i] = crc;
        }
        table
    };
}

pub struct BytecodeSerializer;

impl BytecodeSerializer {
    pub fn op_to_bytes(operation: &Op) -> Vec<u8> {
        let mut bytes: Vec<u8> = vec![];

        bytes.extend(START_MAGIC_BYTES.iter());
        match operation {
            Op::GET { timestamp, key } => {
                let header = 0 as u8 | PROTOCOL_VERSION | GET_OPERATION;
                bytes.push(header);

                let timestamp_bytes = Self::convert_to_varint(*timestamp as usize);
                bytes.extend(timestamp_bytes);

                let key_bytes = key.as_bytes();
                bytes.extend(Self::convert_to_varint(key_bytes.len()));
                bytes.extend(key_bytes);

                let crc = Self::calculate_crc32(&bytes[4..]);
                let crc_bytes = crc.to_le_bytes();
                let crc_len = crc_bytes.len();
                let crc_var_int = Self::convert_to_varint(crc_len);
                bytes.extend(crc_var_int);
                bytes.extend(crc_bytes);

                bytes.extend(END_MAGIC_BYTES.iter());
                bytes
            }
            Op::DEL { timestamp, key } => {
                let header = 0 as u8 | PROTOCOL_VERSION | DEL_OPERATION;
                bytes.push(header);

                let timestamp_bytes = Self::convert_to_varint(*timestamp as usize);
                bytes.extend(timestamp_bytes);

                let key_bytes = key.as_bytes();
                bytes.extend(Self::convert_to_varint(key_bytes.len()));
                bytes.extend(key_bytes);

                let crc = Self::calculate_crc32(&bytes[4..]);
                let crc_bytes = crc.to_le_bytes();
                let crc_len = crc_bytes.len();
                let crc_var_int = Self::convert_to_varint(crc_len);
                bytes.extend(crc_var_int);
                bytes.extend(crc_bytes);

                bytes.extend(END_MAGIC_BYTES.iter());
                bytes
            }
            Op::SET {
                timestamp,
                key,
                value,
            } => {
                let header = 0 as u8 | (PROTOCOL_VERSION << 4) | SET_OPERATION;
                bytes.push(header);

                let timestamp_bytes = Self::convert_to_varint(*timestamp as usize);
                bytes.extend(timestamp_bytes);

                let key_bytes = key.as_bytes();
                bytes.extend(Self::convert_to_varint(key_bytes.len()));
                bytes.extend(key_bytes);

                let value_bytes = value.as_bytes();
                bytes.extend(Self::convert_to_varint(value_bytes.len()));
                bytes.extend(value_bytes);

                let crc = Self::calculate_crc32(&bytes[4..]);
                let crc_bytes = crc.to_le_bytes();
                let crc_len = crc_bytes.len();
                let crc_var_int = Self::convert_to_varint(crc_len);
                bytes.extend(crc_var_int);
                bytes.extend(crc_bytes);
                let magic_end_bytes = END_MAGIC.to_le_bytes();
                bytes.extend(magic_end_bytes);
                bytes
            }
        }
    }

    pub fn op_from_bytes(bytes: Vec<u8>) -> Result<Op, BytecodeSerializerError> {
        let header = bytes
            .get(0)
            .ok_or_else(|| BytecodeSerializerError::SerializationError("No header".to_string()))?;
        let _protocol = header & PROTOCOL_BITMASK;
        let operation = header & OPERATION_BITMASK;
        let mut index = 1usize;

        let (timestamp, idx) = BytecodeSerializer::convert_from_varint(&bytes[1..]);
        let timestamp = timestamp as i64;
        index += idx;
        let (key_len, idx) = BytecodeSerializer::convert_from_varint(&bytes[index..]);

        let key = String::from_utf8(bytes[index + idx..index + idx + key_len].to_vec())
            .map_err(|_| BytecodeSerializerError::SerializationError("Invalid key".to_string()))?;
        println!("{}", key);
        index += idx + key_len;
        match operation {
            SET_OPERATION => {
                let (value_len, idx) = BytecodeSerializer::convert_from_varint(&bytes[index..]);
                let value = String::from_utf8(bytes[index + idx..index + idx + value_len].to_vec())
                    .map_err(|_| {
                        BytecodeSerializerError::SerializationError("Invalid value".to_string())
                    })?;
                index += idx + value_len;
                let (crc_len, idx) = BytecodeSerializer::convert_from_varint(&bytes[index..]);
                let crc = u32::from_le_bytes(
                    bytes[index + idx..index + idx + crc_len]
                        .try_into()
                        .map_err(|_| {
                            BytecodeSerializerError::SerializationError("Invalid crc".to_string())
                        })?,
                );
                let bytes_before_crc = &bytes[..index];
                Self::validate_crc32(bytes_before_crc, crc)?;

                Ok(Op::SET {
                    timestamp,
                    key,
                    value,
                })
            }
            GET_OPERATION => {
                let (crc_len, idx) = BytecodeSerializer::convert_from_varint(&bytes[index..]);
                let crc = u32::from_le_bytes(
                    bytes[index + idx..index + idx + crc_len]
                        .try_into()
                        .map_err(|_| {
                            BytecodeSerializerError::SerializationError("Invalid crc".to_string())
                        })?,
                );
                let bytes_before_crc = &bytes[..index];
                BytecodeSerializer::validate_crc32(bytes_before_crc, crc)?;
                Ok(Op::GET { timestamp, key })
            }
            DEL_OPERATION => {
                let (crc_len, idx) = BytecodeSerializer::convert_from_varint(&bytes[index..]);
                let crc = u32::from_le_bytes(
                    bytes[index + idx..index + idx + crc_len]
                        .try_into()
                        .map_err(|_| {
                            BytecodeSerializerError::SerializationError("Invalid crc".to_string())
                        })?,
                );
                let bytes_before_crc = &bytes[..index];
                BytecodeSerializer::validate_crc32(bytes_before_crc, crc)?;

                Ok(Op::DEL { timestamp, key })
            }
            _ => Err(BytecodeSerializerError::SerializationError(
                "Invalid operation".to_string(),
            )),
        }
    }

    fn convert_to_varint(number: usize) -> Vec<u8> {
        if number == 0 {
            return vec![0];
        }
        let mut result: Vec<u8> = vec![];
        let mut number = number as u64;
        while number != 0 {
            let mut byte = (number & 0b0111_1111) as u8;
            number >>= 7;
            if number != 0 {
                byte |= 0b1000_0000;
            }
            result.push(byte);
        }
        result
    }

    fn convert_from_varint(bytes: &[u8]) -> (usize, usize) {
        let mut result = 0;
        let mut shift = 0;
        let mut index = 0;
        loop {
            let byte = bytes[index];
            result |= ((byte & 0b0111_1111) as usize) << shift;
            if byte & 0b1000_0000 == 0 {
                break;
            }
            shift += 7;
            index += 1;
        }
        (result, index + 1)
    }

    fn calculate_crc32(bytes: &[u8]) -> u32 {
        let mut crc = 0xffffffff;
        for byte in bytes {
            let index = (crc ^ (*byte as u32)) & 0xff;
            crc = LOOKUP_CRC_TABLE[index as usize] ^ (crc >> 8);
        }
        crc ^ 0xffffffff
    }

    fn validate_crc32(bytes: &[u8], crc: u32) -> Result<(), BytecodeSerializerError> {
        let crc_calculated = Self::calculate_crc32(bytes);
        if crc != crc_calculated {
            return Err(BytecodeSerializerError::SerializationError(
                "Invalid crc".to_string(),
            ));
        }
        Ok(())
    }

    fn split_to_chunks(bytes: &[u8]) -> Vec<Vec<u8>> {
        let magic_start = START_MAGIC.to_le_bytes();
        let magic_end = END_MAGIC.to_le_bytes();
        let mut chunks: Vec<Vec<u8>> = vec![];
        let mut start = 0;
        let mut index = 0;
        let mut chunk: Vec<u8> = vec![];
        if (bytes.len() == 0) {
            return vec![];
        }
        while index <= bytes.len() - 4 {
            if bytes[index..index + 4] == magic_start {
                start = index + 4
            }
            if bytes[index..index + 4] == magic_end && start != 0 {
                chunk.extend_from_slice(&bytes[start..index]);
                chunks.push(chunk.clone());
                chunk.clear();
            }
            index += 1
        }

        chunks
    }

    pub fn recover_from_chunks(chunks: Vec<Vec<u8>>) -> Result<Vec<Op>, BytecodeSerializerError> {
        let mut ops: Vec<Op> = vec![];
        for chunk in chunks {
            let op = BytecodeSerializer::op_from_bytes(chunk)?;
            ops.push(op);
        }
        Ok(ops)
    }

    pub fn recover_from_bytes(bytes: &[u8]) -> Result<Vec<Op>, BytecodeSerializerError> {
        let chunks = Self::split_to_chunks(bytes);
        Self::recover_from_chunks(chunks)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_conver_to_varint() {
        let number = 127;
        let expected = vec![0b0111_1111];
        let result = BytecodeSerializer::convert_to_varint(number);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_conver_to_varint_2() {
        let number = 128;
        let expected = vec![0b1000_0000, 0b0000_0001];
        let result = BytecodeSerializer::convert_to_varint(number);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_conver_to_varint_3() {
        let number = 300;
        let expected = vec![0b1010_1100, 0b0000_0010];
        let result = BytecodeSerializer::convert_to_varint(number);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_conver_to_varint_4() {
        let number = 16384;
        let expected = vec![0b1000_0000, 0b1000_0000, 0b0000_0001];
        let result = BytecodeSerializer::convert_to_varint(number);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_conver_to_varint_5() {
        let number = 2097152;
        let expected = vec![0b1000_0000, 0b1000_0000, 0b1000_0000, 0b0000_0001];
        let result = BytecodeSerializer::convert_to_varint(number);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_convert_from_varint() {
        let bytes = vec![0b0111_1111];
        let expected = (127, 1);
        let result = BytecodeSerializer::convert_from_varint(&bytes);

        assert_eq!(result, expected);
    }

    #[test]
    fn test_convert_from_varint_2() {
        let bytes = vec![0b1000_0000, 0b0000_0001];
        let expected = (128, 2);
        let result = BytecodeSerializer::convert_from_varint(&bytes);

        assert_eq!(result, expected);
    }

    #[test]
    fn test_convert_from_varint_3() {
        let bytes = vec![0b1010_1100, 0b0000_0010];
        let expected = (300, 2);
        let result = BytecodeSerializer::convert_from_varint(&bytes);

        assert_eq!(result, expected);
    }

    #[test]
    fn test_convert_from_varint_4() {
        let bytes = vec![0b1000_0000, 0b1000_0000, 0b0000_0001];
        let expected = (16384, 3);
        let result = BytecodeSerializer::convert_from_varint(&bytes);

        assert_eq!(result, expected);
    }

    #[test]
    fn test_convert_from_varint_5() {
        let bytes = vec![0b1000_0000, 0b1000_0000, 0b1000_0000, 0b0000_0001];
        let expected = (2097152, 4);
        let result = BytecodeSerializer::convert_from_varint(&bytes);

        assert_eq!(result, expected);
    }

    #[test]
    fn test_convert_from_varint_6() {
        let bytes = vec![0b0000_0000, 0b1000_0000, 0b1000_0000, 0b0000_0001];
        let expected = (0, 1);
        let result = BytecodeSerializer::convert_from_varint(&bytes);

        assert_eq!(result, expected);
    }

    #[test]
    fn test_op_to_bytes() {
        let op = Op::GET {
            key: "key".to_string(),
            timestamp: 1234567890,
        };
        let op_bytes = BytecodeSerializer::op_to_bytes(&op);
        let expected = vec![
            237, 200, 254, 222, 2, 210, 133, 216, 204, 4, 3, 107, 101, 121, 4, 50, 178, 183, 170,
            229, 177, 0, 11,
        ];
        assert_eq!(op_bytes, expected);
    }

    #[test]
    fn test_op_from_bytes() {
        let op = Op::GET {
            key: "key".to_string(),
            timestamp: 1234567890,
        };
        let op_result = BytecodeSerializer::op_from_bytes(vec![
            2, 210, 133, 216, 204, 4, 3, 107, 101, 121, 4, 50, 178, 183, 170,
        ])
        .unwrap();
        assert_eq!(op, op_result);
    }

    #[test]
    fn test_split_to_chunks() {
        let magic_start = START_MAGIC.to_le_bytes();
        let magic_end = END_MAGIC.to_le_bytes();
        let mut test_buff: Vec<u8> = vec![];
        let mut chunks: Vec<Vec<u8>> = vec![];
        for x in 1690..1710 {
            let x = x as i32;
            let slice = x.to_le_bytes().to_vec();
            chunks.push(slice)
        }

        for chunk in &chunks {
            test_buff.extend_from_slice(&magic_start);
            test_buff.extend_from_slice(&chunk);
            test_buff.extend_from_slice(&magic_end);
        }

        let chunks_control = BytecodeSerializer::split_to_chunks(&test_buff);
        assert_eq!(chunks, chunks_control);
    }
}
