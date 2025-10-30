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
//! - **Optional tokio integration**: Use `tokio-runtime` feature for async progress tracking
//!
//! ## Cargo Features
//!
//! - `tokio-runtime` (optional): Enables async/await support with tokio for progress tracking.
//!   Without this feature, the library is zero-dependency and uses synchronous APIs.
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
//! ### Progress Tracking for UI Integration (Synchronous)
//!
//! For applications with UI that need to display progress while reading or writing,
//! without requiring tokio:
//!
//! ```no_run
//! use wpilog_parser::WpilogReader;
//! use std::thread;
//!
//! let reader = WpilogReader::from_file("data.wpilog")?;
//! let (records, progress_rx) = reader.read_all_with_progress();
//!
//! // Optionally spawn a thread to handle progress updates
//! let progress_thread = thread::spawn(move || {
//!     while let Ok(update) = progress_rx.recv() {
//!         match update {
//!             wpilog_parser::ProgressUpdate::Progress {
//!                 percent,
//!                 processed,
//!                 total,
//!                 current_phase,
//!             } => {
//!                 println!("{}: {:.1}% ({}/{})", current_phase, percent, processed, total);
//!                 // Update UI progress bar here
//!             }
//!             wpilog_parser::ProgressUpdate::Complete { total_processed } => {
//!                 println!("Completed! Processed {} items", total_processed);
//!             }
//!             wpilog_parser::ProgressUpdate::Error { message } => {
//!                 eprintln!("Error: {}", message);
//!             }
//!             _ => {}
//!         }
//!     }
//! });
//!
//! println!("Read {} records", records.len());
//! progress_thread.join().ok();
//! # Ok::<(), wpilog_parser::Error>(())
//! ```
//!
//! ### Progress Tracking with Async/Await (Requires `tokio-runtime` feature)
//!
//! For applications using tokio's async runtime that need async progress tracking:
//!
//! ```no_run
//! # #[cfg(feature = "tokio-runtime")]
//! # {
//! use wpilog_parser::WpilogReader;
//! use tokio::sync::mpsc;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let reader = WpilogReader::from_file("data.wpilog")?;
//!
//!     // Get async future and progress channel
//!     let (result, mut progress_rx) = reader.read_all_with_progress_async();
//!
//!     // Spawn task to handle progress updates in your UI
//!     let ui_handle = tokio::spawn(async move {
//!         while let Some(update) = progress_rx.recv().await {
//!             match update {
//!                 wpilog_parser::ProgressUpdate::Progress {
//!                     percent,
//!                     processed,
//!                     total,
//!                     current_phase,
//!                 } => {
//!                     println!("{}: {:.1}% ({}/{})", current_phase, percent, processed, total);
//!                     // Update UI progress bar here
//!                 }
//!                 wpilog_parser::ProgressUpdate::Complete { total_processed } => {
//!                     println!("Completed! Processed {} items", total_processed);
//!                 }
//!                 wpilog_parser::ProgressUpdate::Error { message } => {
//!                     eprintln!("Error: {}", message);
//!                 }
//!                 _ => {}
//!             }
//!         }
//!     });
//!
//!     // Wait for reading to complete
//!     let records = result.await?;
//!     println!("Read {} records", records.len());
//!
//!     ui_handle.await?;
//!     Ok(())
//! }
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! # }
//! ```
//!
//! #### Writing with Progress (Requires `tokio-runtime` feature)
//!
//! Similarly, for writing Parquet files with async progress reporting:
//!
//! ```no_run
//! # #[cfg(feature = "tokio-runtime")]
//! # {
//! use wpilog_parser::{WpilogReader, ParquetWriter};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let reader = WpilogReader::from_file("data.wpilog")?;
//!     let records = reader.read_all()?;
//!
//!     let writer = ParquetWriter::new("./output");
//!     let (result, mut progress_rx) = writer.write_with_progress_async(&records);
//!
//!     // Handle progress updates
//!     let ui_handle = tokio::spawn(async move {
//!         while let Some(update) = progress_rx.recv().await {
//!             match update {
//!                 wpilog_parser::ProgressUpdate::Progress { percent, .. } => {
//!                     println!("Write progress: {:.1}%", percent);
//!                 }
//!                 _ => {}
//!             }
//!         }
//!     });
//!
//!     let stats = result.await?;
//!     println!("{}", stats.summary());
//!
//!     ui_handle.await?;
//!     Ok(())
//! }
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! # }
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
pub mod progress;
pub mod reader;
pub mod writer;

// Re-export commonly used types
pub use error::{Error, Result};
pub use progress::{ProgressTracker, ProgressUpdate};
pub use reader::{WpilogReader, WpilogReaderBuilder};
pub use writer::{ParquetWriter, ParquetWriterBuilder, WriteStats};

// Re-export models for users who need them
pub use models::{OutputFormat, WideRow};

// Internal modules (public but not part of the high-level API)
pub mod datalog;
pub mod formats;
pub mod formatter;
pub mod models;

// Convenience type aliases
/// Alias for the result of reading WPILog records
pub type Records = Vec<WideRow>;
