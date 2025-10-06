//! Error types for the WPILog parser library.

use std::fmt;

/// Result type alias for WPILog operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors that can occur when parsing or writing WPILog files.
#[derive(Debug)]
pub enum Error {
    /// Invalid WPILog file format (e.g., wrong magic bytes, unsupported version)
    InvalidFormat(String),

    /// I/O error occurred while reading or writing
    Io(std::io::Error),

    /// Entry not found or invalid entry ID
    InvalidEntry(String),

    /// Data parsing error (e.g., wrong data type, corrupted data)
    ParseError(String),

    /// Schema inference or validation error
    SchemaError(String),

    /// Output format error (e.g., Parquet write error)
    OutputError(String),

    /// UTF-8 encoding/decoding error
    Utf8Error(std::string::FromUtf8Error),

    /// Generic error with message
    Other(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::InvalidFormat(msg) => write!(f, "Invalid WPILOG format: {}", msg),
            Error::Io(err) => write!(f, "I/O error: {}", err),
            Error::InvalidEntry(msg) => write!(f, "Invalid entry: {}", msg),
            Error::ParseError(msg) => write!(f, "Parse error: {}", msg),
            Error::SchemaError(msg) => write!(f, "Schema error: {}", msg),
            Error::OutputError(msg) => write!(f, "Output error: {}", msg),
            Error::Utf8Error(err) => write!(f, "UTF-8 error: {}", err),
            Error::Other(msg) => write!(f, "{}", msg),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(err) => Some(err),
            Error::Utf8Error(err) => Some(err),
            _ => None,
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::Io(err)
    }
}

impl From<std::string::FromUtf8Error> for Error {
    fn from(err: std::string::FromUtf8Error) -> Self {
        Error::Utf8Error(err)
    }
}

impl From<anyhow::Error> for Error {
    fn from(err: anyhow::Error) -> Self {
        Error::Other(err.to_string())
    }
}
