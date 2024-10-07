use std::string::FromUtf8Error;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum PersistentLayerError {
    #[error("Error creating block header, cause: {0}")]
    HeaderCrError(String),

    #[error("LinePointer Key length exceeded, should be {0} max, but actual is {1}")]
    LinePointerLenError(usize, usize),

    #[error("Byte slice is too short!")]
    LinePointerSerializationError,

    #[error("UTF8 error")]
    FailedParsingUTF8(#[from] FromUtf8Error),

    #[error("Value too long")]
    ValueTooLong,

    #[error("Header error: {0}")]
    HeaderError(String),
}
