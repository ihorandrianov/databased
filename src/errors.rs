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

#[derive(Error, Debug)]
pub enum ParserError {
    #[error("Error parsing token")]
    TokenParseError(String),

    #[error("Error parsing command")]
    CommandParseError(String),

    #[error("Error parsing key")]
    KeyParseError(String),

    #[error("Error parsing value")]
    ValueParseError(String),

    #[error("Error serializing from UTF8")]
    UTF8Error(#[from] Utf8Error),

    #[error("Error reading from buffer")]
    BufferError(#[from] std::io::Error),

    #[error("No operations found")]
    NoOperations,
}

#[derive(Error, Debug)]
pub enum WALError {
    #[error("Error writing to WAL")]
    WriteError(#[from] std::io::Error),

    #[error("Error reading from WAL")]
    ReadError(String),
}

#[derive(Error, Debug)]
pub enum FileSystemError {
    #[error("Error creating directory {0}")]
    CreateDir(String),
}

#[derive(Error, Debug)]
pub enum PersistentLayerError {
    #[error("Error writing or reading from disk")]
    DiskError(#[from] std::io::Error),

    #[error("Error serializing or deserializing data, message: {0}")]
    SerializationError(String),

    #[error("Something went wrong: {0}")]
    GenericError(String),
}

#[derive(Debug)]
pub enum KVStoreError {
    MemoryLayerError(MemoryLayerErrors),
    BytecodeSerializerError(BytecodeSerializerError),
    ParserError(ParserError),
    WALError(WALError),
    FileSystemError(FileSystemError),
    PersistentLayerError(PersistentLayerError),
    LogError(LogError),
}
