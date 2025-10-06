//! High-level API for reading WPILog files.

use crate::datalog::DataLogReader;
use crate::error::{Error, Result};
use crate::formatter::Formatter;
use crate::models::{OutputFormat, WideRow};
use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

static GLOBAL_LOOP_COUNT: AtomicU64 = AtomicU64::new(0);

/// A reader for WPILog files that provides a high-level API for parsing.
///
/// # Examples
///
/// ```no_run
/// use wpilog_parser::WpilogReader;
///
/// // Read from a file
/// let reader = WpilogReader::from_file("data.wpilog")?;
/// let records = reader.read_all()?;
/// # Ok::<(), wpilog_parser::Error>(())
/// ```
pub struct WpilogReader {
    data: Vec<u8>,
    formatter: Option<Formatter>,
}

impl WpilogReader {
    /// Create a new WPILog reader from a file path.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the .wpilog file
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be read or is not a valid WPILog file.
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let mut file = File::open(path.as_ref())?;
        let mut data = Vec::new();
        file.read_to_end(&mut data)?;

        let reader = DataLogReader::new(&data);
        if !reader.is_valid() {
            return Err(Error::InvalidFormat(
                "Not a valid WPILOG file".to_string(),
            ));
        }

        Ok(Self {
            data,
            formatter: None,
        })
    }

    /// Create a new WPILog reader from raw bytes.
    ///
    /// # Arguments
    ///
    /// * `data` - Raw bytes of the WPILog file
    ///
    /// # Errors
    ///
    /// Returns an error if the data is not a valid WPILog file.
    pub fn from_bytes(data: Vec<u8>) -> Result<Self> {
        let reader = DataLogReader::new(&data);
        if !reader.is_valid() {
            return Err(Error::InvalidFormat(
                "Not a valid WPILOG file".to_string(),
            ));
        }

        Ok(Self {
            data,
            formatter: None,
        })
    }

    /// Get the WPILog file version.
    ///
    /// Returns the version number as a 16-bit integer (e.g., 0x0100 for version 1.0).
    pub fn version(&self) -> u16 {
        let reader = DataLogReader::new(&self.data);
        reader.get_version()
    }

    /// Get the extra header string from the WPILog file.
    ///
    /// The extra header is an optional UTF-8 string that can contain arbitrary metadata.
    pub fn extra_header(&self) -> String {
        let reader = DataLogReader::new(&self.data);
        reader.get_extra_header()
    }

    /// Read all records from the WPILog file in wide format.
    ///
    /// In wide format, each row contains a timestamp and all metric values at that timestamp.
    /// This is the most common format for analysis.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be parsed or contains invalid data.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use wpilog_parser::WpilogReader;
    ///
    /// let reader = WpilogReader::from_file("data.wpilog")?;
    /// let records = reader.read_all()?;
    ///
    /// println!("Read {} records", records.len());
    /// for record in records.iter().take(5) {
    ///     println!("Timestamp: {}, Entry: {}", record.timestamp, record.entry);
    /// }
    /// # Ok::<(), wpilog_parser::Error>(())
    /// ```
    pub fn read_all(mut self) -> Result<Vec<WideRow>> {
        // Reset global loop count
        GLOBAL_LOOP_COUNT.store(0, Ordering::Relaxed);

        let mut formatter = Formatter::new(
            String::new(), // file path not used anymore
            String::new(), // output_directory not used
            OutputFormat::Wide,
        );

        // First pass: infer schema
        formatter
            .read_wpilog_from_bytes(&self.data, true)
            .map_err(|e| Error::SchemaError(e.to_string()))?;

        // Reset loop count for second pass
        Formatter::reset_loop_count();

        // Second pass: read data
        let records = formatter
            .read_wpilog_from_bytes(&self.data, false)
            .map_err(|e| Error::ParseError(e.to_string()))?;

        self.formatter = Some(formatter);
        Ok(records)
    }

    /// Read all records with access to the internal formatter for advanced use cases.
    ///
    /// This method gives you access to the formatter which contains metadata like
    /// metric names and struct schemas.
    ///
    /// # Returns
    ///
    /// A tuple of (records, formatter) where formatter contains additional metadata.
    pub fn read_all_with_metadata(self) -> Result<(Vec<WideRow>, Formatter)> {
        // Reset global loop count
        GLOBAL_LOOP_COUNT.store(0, Ordering::Relaxed);

        let mut formatter = Formatter::new(
            String::new(),
            String::new(),
            OutputFormat::Wide,
        );

        // First pass: infer schema
        formatter
            .read_wpilog_from_bytes(&self.data, true)
            .map_err(|e| Error::SchemaError(e.to_string()))?;

        // Reset loop count
        Formatter::reset_loop_count();

        // Second pass: read data
        let records = formatter
            .read_wpilog_from_bytes(&self.data, false)
            .map_err(|e| Error::ParseError(e.to_string()))?;

        Ok((records, formatter))
    }

    /// Get a low-level reader for advanced parsing operations.
    ///
    /// This gives you direct access to the underlying binary parser for
    /// custom parsing logic or performance-critical applications.
    pub fn low_level_reader(&self) -> DataLogReader<'_> {
        DataLogReader::new(&self.data)
    }
}

/// Builder for configuring WPILog parsing options.
///
/// # Examples
///
/// ```no_run
/// use wpilog_parser::WpilogReaderBuilder;
///
/// let reader = WpilogReaderBuilder::new()
///     .from_file("data.wpilog")?;
/// let records = reader.read_all()?;
/// # Ok::<(), wpilog_parser::Error>(())
/// ```
pub struct WpilogReaderBuilder {
    output_format: OutputFormat,
}

impl WpilogReaderBuilder {
    /// Create a new reader builder with default options.
    pub fn new() -> Self {
        Self {
            output_format: OutputFormat::Wide,
        }
    }

    /// Set the output format (Wide or Long).
    ///
    /// Default is Wide format.
    pub fn output_format(mut self, format: OutputFormat) -> Self {
        self.output_format = format;
        self
    }

    /// Build a reader from a file path.
    pub fn from_file<P: AsRef<Path>>(self, path: P) -> Result<WpilogReader> {
        WpilogReader::from_file(path)
    }

    /// Build a reader from raw bytes.
    pub fn from_bytes(self, data: Vec<u8>) -> Result<WpilogReader> {
        WpilogReader::from_bytes(data)
    }
}

impl Default for WpilogReaderBuilder {
    fn default() -> Self {
        Self::new()
    }
}
