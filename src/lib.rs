//! # WPILog Parser
//!
//! A high-performance Rust library for parsing WPILib data log files (`.wpilog`) and converting them
//! to various output formats like Apache Parquet.
//!
//! ## Features
//!
//! - **Fast parsing**: Zero-copy parsing with memory-mapped files
//! - **Type safety**: Strong typing for all WPILog data types
//! - **Parquet output**: Efficient columnar storage with proper type inference
//! - **Array support**: Native support for all WPILog array types
//! - **Struct schemas**: Parsing and unpacking of nested struct data
//! - **UTF-8 support**: Full Unicode support for names and values
//!
//! ## Quick Start
//!
//! ```no_run
//! use wpilog_parser::{WpilogReader, ParquetWriter};
//!
//! // Read a WPILog file
//! let reader = WpilogReader::from_file("data.wpilog")?;
//! let records = reader.read_all()?;
//!
//! println!("Read {} records", records.len());
//!
//! // Write to Parquet format
//! ParquetWriter::new("output_directory")
//!     .chunk_size(100_000)
//!     .write(&records)?;
//! # Ok::<(), wpilog_parser::Error>(())
//! ```
//!
//! ## Data Types
//!
//! The library supports all standard WPILog data types:
//!
//! - **Scalars**: `boolean`, `int64`, `float`, `double`, `string`
//! - **Arrays**: `boolean[]`, `int64[]`, `float[]`, `double[]`, `string[]`
//! - **Complex**: `json`, `msgpack`, struct types
//!
//! ## Output Formats
//!
//! ### Parquet
//!
//! The primary output format with proper type inference:
//!
//! ```no_run
//! use wpilog_parser::{WpilogReader, ParquetWriter};
//!
//! let reader = WpilogReader::from_file("data.wpilog")?;
//! let records = reader.read_all()?;
//!
//! let stats = ParquetWriter::new("./output")
//!     .write_with_stats(&records)?;
//!
//! println!("{}", stats.summary());
//! # Ok::<(), wpilog_parser::Error>(())
//! ```
//!
//! ## Advanced Usage
//!
//! ### Low-Level Access
//!
//! For performance-critical applications or custom parsing logic:
//!
//! ```no_run
//! use wpilog_parser::WpilogReader;
//!
//! let reader = WpilogReader::from_file("data.wpilog")?;
//! let low_level = reader.low_level_reader();
//!
//! for record_result in low_level.records()? {
//!     let record = record_result?;
//!     // Custom processing...
//! }
//! # Ok::<(), wpilog_parser::Error>(())
//! ```
//!
//! ### Accessing Metadata
//!
//! Get metric names and struct schemas:
//!
//! ```no_run
//! use wpilog_parser::WpilogReader;
//!
//! let reader = WpilogReader::from_file("data.wpilog")?;
//! let (records, formatter) = reader.read_all_with_metadata()?;
//!
//! println!("Found {} unique metrics", formatter.metrics_names.len());
//! println!("Parsed {} struct schemas", formatter.struct_schemas.len());
//! # Ok::<(), wpilog_parser::Error>(())
//! ```
//!
//! ## Error Handling
//!
//! All operations return `Result<T, Error>` for comprehensive error handling:
//!
//! ```no_run
//! use wpilog_parser::{WpilogReader, Error};
//!
//! match WpilogReader::from_file("data.wpilog") {
//!     Ok(reader) => {
//!         // Process the file...
//!     }
//!     Err(Error::InvalidFormat(msg)) => {
//!         eprintln!("Invalid WPILOG file: {}", msg);
//!     }
//!     Err(Error::Io(err)) => {
//!         eprintln!("I/O error: {}", err);
//!     }
//!     Err(err) => {
//!         eprintln!("Error: {}", err);
//!     }
//! }
//! # Ok::<(), wpilog_parser::Error>(())
//! ```

// Public API modules
pub mod error;
pub mod reader;
pub mod writer;

// Re-export commonly used types
pub use error::{Error, Result};
pub use reader::{WpilogReader, WpilogReaderBuilder};
pub use writer::{ParquetWriter, ParquetWriterBuilder, WriteStats};

// Re-export models for users who need them
pub use models::{OutputFormat, WideRow};

// Internal modules (public but not part of the high-level API)
pub mod datalog;
pub mod formatter;
pub mod formats;
pub mod models;

// Convenience type aliases
/// Alias for the result of reading WPILog records
pub type Records = Vec<WideRow>;
