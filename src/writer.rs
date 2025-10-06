//! High-level API for writing parsed WPILog data to various formats.

use crate::error::{Error, Result};
use crate::formats::parquet::ParquetFormatter;
use crate::models::WideRow;
use std::path::Path;

/// Writer for outputting WPILog data to Apache Parquet format.
///
/// Parquet is a columnar storage format optimized for analytics queries.
/// It provides excellent compression and is widely supported by data tools.
///
/// # Examples
///
/// ```no_run
/// use wpilog_parser::{WpilogReader, ParquetWriter};
///
/// // Read WPILog file
/// let reader = WpilogReader::from_file("data.wpilog")?;
/// let records = reader.read_all()?;
///
/// // Write to Parquet
/// ParquetWriter::new("output_dir")
///     .write(&records)?;
/// # Ok::<(), wpilog_parser::Error>(())
/// ```
pub struct ParquetWriter {
    output_directory: String,
    chunk_size: usize,
}

impl ParquetWriter {
    /// Create a new Parquet writer that will write to the specified directory.
    ///
    /// # Arguments
    ///
    /// * `output_directory` - Directory where Parquet files will be written
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use wpilog_parser::ParquetWriter;
    ///
    /// let writer = ParquetWriter::new("./output");
    /// ```
    pub fn new<P: AsRef<Path>>(output_directory: P) -> Self {
        Self {
            output_directory: output_directory.as_ref().to_string_lossy().to_string(),
            chunk_size: 50_000, // Default chunk size
        }
    }

    /// Set the chunk size for splitting large datasets.
    ///
    /// Large datasets are split into multiple Parquet files to avoid memory issues
    /// and improve parallel processing. Default is 50,000 rows per file.
    ///
    /// # Arguments
    ///
    /// * `size` - Number of rows per Parquet file
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use wpilog_parser::ParquetWriter;
    ///
    /// let writer = ParquetWriter::new("./output")
    ///     .chunk_size(100_000);
    /// ```
    pub fn chunk_size(mut self, size: usize) -> Self {
        self.chunk_size = size;
        self
    }

    /// Write the records to Parquet format.
    ///
    /// This will create one or more Parquet files in the output directory,
    /// named `file_part000.parquet`, `file_part001.parquet`, etc.
    ///
    /// # Arguments
    ///
    /// * `records` - The WPILog records to write
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The output directory cannot be created
    /// - The Parquet files cannot be written
    /// - The records are empty
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use wpilog_parser::{WpilogReader, ParquetWriter};
    ///
    /// let reader = WpilogReader::from_file("data.wpilog")?;
    /// let records = reader.read_all()?;
    ///
    /// ParquetWriter::new("./output")
    ///     .chunk_size(100_000)
    ///     .write(&records)?;
    /// # Ok::<(), wpilog_parser::Error>(())
    /// ```
    pub fn write(self, records: &[WideRow]) -> Result<()> {
        let formatter = ParquetFormatter::new(self.output_directory, self.chunk_size);

        formatter
            .convert(records)
            .map_err(|e| Error::OutputError(e.to_string()))?;

        Ok(())
    }

    /// Write records to Parquet and return statistics about the write operation.
    ///
    /// # Returns
    ///
    /// A `WriteStats` struct containing information about the write operation.
    pub fn write_with_stats(self, records: &[WideRow]) -> Result<WriteStats> {
        let num_records = records.len();
        let num_chunks = (num_records + self.chunk_size - 1) / self.chunk_size;
        let chunk_size = self.chunk_size;

        self.write(records)?;

        Ok(WriteStats {
            num_records,
            num_chunks,
            chunk_size,
        })
    }
}

/// Statistics about a Parquet write operation.
#[derive(Debug, Clone)]
pub struct WriteStats {
    /// Total number of records written
    pub num_records: usize,
    /// Number of Parquet files created
    pub num_chunks: usize,
    /// Rows per file (chunk size)
    pub chunk_size: usize,
}

impl WriteStats {
    /// Get a human-readable summary of the write operation.
    pub fn summary(&self) -> String {
        format!(
            "Wrote {} records across {} file(s) ({} rows per file)",
            self.num_records, self.num_chunks, self.chunk_size
        )
    }
}

/// Builder for configuring Parquet write options.
///
/// # Examples
///
/// ```no_run
/// use wpilog_parser::{WpilogReader, ParquetWriterBuilder};
///
/// let reader = WpilogReader::from_file("data.wpilog")?;
/// let records = reader.read_all()?;
///
/// ParquetWriterBuilder::new()
///     .output_directory("./output")
///     .chunk_size(75_000)
///     .build()?
///     .write(&records)?;
/// # Ok::<(), wpilog_parser::Error>(())
/// ```
pub struct ParquetWriterBuilder {
    output_directory: Option<String>,
    chunk_size: usize,
}

impl ParquetWriterBuilder {
    /// Create a new Parquet writer builder with default options.
    pub fn new() -> Self {
        Self {
            output_directory: None,
            chunk_size: 50_000,
        }
    }

    /// Set the output directory.
    pub fn output_directory<P: AsRef<Path>>(mut self, path: P) -> Self {
        self.output_directory = Some(path.as_ref().to_string_lossy().to_string());
        self
    }

    /// Set the chunk size.
    pub fn chunk_size(mut self, size: usize) -> Self {
        self.chunk_size = size;
        self
    }

    /// Build the Parquet writer.
    ///
    /// # Errors
    ///
    /// Returns an error if output_directory was not set.
    pub fn build(self) -> Result<ParquetWriter> {
        let output_directory = self
            .output_directory
            .ok_or_else(|| Error::Other("Output directory not set".to_string()))?;

        Ok(ParquetWriter {
            output_directory,
            chunk_size: self.chunk_size,
        })
    }
}

impl Default for ParquetWriterBuilder {
    fn default() -> Self {
        Self::new()
    }
}
