use std::str::Utf8Error;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum LogError {
    #[error("UTF8 error")]
    LogUTFError(#[from] Utf8Error),

    #[error("IO error")]
    IO(#[from] std::io::Error),

    #[error("Parsing error problems:{0}")]
    LogParseError(String),

    #[error("Error parsing timestamp")]
    TimestampError(#[from] std::num::ParseIntError),
}

#[derive(Error, Debug)]
pub enum MemoryLayerErrors {
    #[error("Something gone wrong: {0}")]
    GenericError(String),
}

#[derive(Error, Debug)]
pub enum BytecodeSerializerError {
    #[error("Error serializing bytecode")]
    SerializationError(String),

    #[error("Error deserializing bytecode")]
    DeserializationError(String),
}
