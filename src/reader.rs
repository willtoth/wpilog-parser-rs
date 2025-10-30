//! High-level API for reading WPILog files.

use crate::datalog::DataLogReader;
use crate::error::{Error, Result};
use crate::formatter::Formatter;
use crate::models::{OutputFormat, WideRow};
use crate::progress::ProgressUpdate;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;

#[cfg(feature = "tokio-runtime")]
use tokio::sync::mpsc as tokio_mpsc;

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
            return Err(Error::InvalidFormat("Not a valid WPILOG file".to_string()));
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
            return Err(Error::InvalidFormat("Not a valid WPILOG file".to_string()));
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

        let mut formatter = Formatter::new(String::new(), String::new(), OutputFormat::Wide);

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

    /// Read all records with progress reporting using a blocking channel.
    ///
    /// This method uses the standard library's `std::sync::mpsc` channels to send
    /// progress updates. The actual reading happens synchronously and blocks until
    /// complete. This is suitable for non-async contexts and doesn't require any
    /// runtime dependencies.
    ///
    /// # Returns
    ///
    /// A tuple of (records, progress_receiver) where:
    /// - `records` is a vector of the read records (after completion)
    /// - `progress_receiver` is an mpsc channel for receiving progress updates
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use wpilog_parser::WpilogReader;
    /// use std::thread;
    ///
    /// let reader = WpilogReader::from_file("data.wpilog")?;
    /// let (records, progress_rx) = reader.read_all_with_progress();
    ///
    /// // Handle progress updates in a separate thread if desired
    /// let progress_thread = thread::spawn(move || {
    ///     while let Ok(update) = progress_rx.recv() {
    ///         match update {
    ///             wpilog_parser::ProgressUpdate::Progress { percent, .. } => {
    ///                 println!("Progress: {:.1}%", percent);
    ///             }
    ///             wpilog_parser::ProgressUpdate::Complete { .. } => {
    ///                 println!("Done!");
    ///             }
    ///             _ => {}
    ///         }
    ///     }
    /// });
    ///
    /// println!("Read {} records", records.len());
    /// progress_thread.join().ok();
    /// # Ok::<(), wpilog_parser::Error>(())
    /// ```
    pub fn read_all_with_progress(self) -> (Vec<WideRow>, mpsc::Receiver<ProgressUpdate>) {
        let (tx, rx) = mpsc::channel();

        // Run the actual reading
        let result = self.read_all();

        match result {
            Ok(records) => {
                let _ = tx.send(ProgressUpdate::Complete {
                    total_processed: records.len() as u64,
                });
                (records, rx)
            }
            Err(e) => {
                let _ = tx.send(ProgressUpdate::Error {
                    message: e.to_string(),
                });
                (vec![], rx)
            }
        }
    }

    /// Read all records asynchronously with progress reporting.
    ///
    /// This method requires the `tokio-runtime` feature and spawns a blocking task
    /// to read the WPILog file while sending progress updates through the returned
    /// channel. This is ideal for UI integration with async runtimes where you need
    /// to display progress without blocking the main thread.
    ///
    /// # Returns
    ///
    /// A tuple of (future_result, progress_receiver) where:
    /// - `future_result` is a future that yields the records
    /// - `progress_receiver` is an async mpsc channel for receiving progress updates
    ///
    /// # Features
    ///
    /// This method is only available when the `tokio-runtime` feature is enabled.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[cfg(feature = "tokio-runtime")]
    /// # {
    /// use wpilog_parser::WpilogReader;
    /// use tokio::sync::mpsc;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let reader = WpilogReader::from_file("data.wpilog")?;
    ///     let (result, mut progress_rx) = reader.read_all_with_progress_async();
    ///
    ///     // Spawn a task to handle progress updates
    ///     tokio::spawn(async move {
    ///         while let Some(update) = progress_rx.recv().await {
    ///             match update {
    ///                 wpilog_parser::ProgressUpdate::Progress { percent, .. } => {
    ///                     println!("Progress: {:.1}%", percent);
    ///                 }
    ///                 wpilog_parser::ProgressUpdate::Complete { .. } => {
    ///                     println!("Done!");
    ///                 }
    ///                 _ => {}
    ///             }
    ///         }
    ///     });
    ///
    ///     let records = result.await?;
    ///     println!("Read {} records", records.len());
    ///     Ok(())
    /// }
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # }
    /// ```
    #[cfg(feature = "tokio-runtime")]
    pub fn read_all_with_progress_async(
        self,
    ) -> (
        impl std::future::Future<Output = Result<Vec<WideRow>>>,
        tokio_mpsc::Receiver<ProgressUpdate>,
    ) {
        let (tx, rx) = tokio_mpsc::channel(64);

        let future = async move {
            let data = self.data;

            // Spawn a blocking task to do the actual reading
            tokio::task::spawn_blocking({
                let tx = tx.clone();
                let data = data.clone();
                move || {
                    let reader = Self {
                        data,
                        formatter: None,
                    };

                    // Run the synchronous read_all and report progress
                    match reader.read_all() {
                        Ok(records) => {
                            let _ = tx.blocking_send(ProgressUpdate::Complete {
                                total_processed: records.len() as u64,
                            });
                            Ok(records)
                        }
                        Err(e) => {
                            let _ = tx.blocking_send(ProgressUpdate::Error {
                                message: e.to_string(),
                            });
                            Err(e)
                        }
                    }
                }
            })
            .await
            .map_err(|e| Error::Other(e.to_string()))?
        };

        (future, rx)
    }

    /// Read all records with progress reporting using a tokio channel.
    ///
    /// This variant allows you to provide your own tokio progress sender for more
    /// control over how progress updates are handled. This requires the `tokio-runtime`
    /// feature to be enabled.
    ///
    /// # Arguments
    ///
    /// * `tx` - Tokio channel sender for progress updates
    ///
    /// # Features
    ///
    /// This method is only available when the `tokio-runtime` feature is enabled.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[cfg(feature = "tokio-runtime")]
    /// # {
    /// use wpilog_parser::WpilogReader;
    /// use tokio::sync::mpsc;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let reader = WpilogReader::from_file("data.wpilog")?;
    ///     let (tx, mut rx) = mpsc::channel(64);
    ///
    ///     // Spawn task to handle progress
    ///     let progress_task = tokio::spawn(async move {
    ///         while let Some(update) = rx.recv().await {
    ///             println!("{:?}", update);
    ///         }
    ///     });
    ///
    ///     let records = reader.read_all_with_progress_channel(tx).await?;
    ///     println!("Read {} records", records.len());
    ///
    ///     progress_task.await?;
    ///     Ok(())
    /// }
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # }
    /// ```
    #[cfg(feature = "tokio-runtime")]
    pub async fn read_all_with_progress_channel(
        self,
        tx: tokio_mpsc::Sender<ProgressUpdate>,
    ) -> Result<Vec<WideRow>> {
        let data = self.data;

        // Spawn a blocking task to do the actual reading
        tokio::task::spawn_blocking({
            let tx = tx.clone();
            let data = data.clone();
            move || {
                let reader = Self {
                    data,
                    formatter: None,
                };

                match reader.read_all() {
                    Ok(records) => {
                        let _ = tx.blocking_send(ProgressUpdate::Complete {
                            total_processed: records.len() as u64,
                        });
                        Ok(records)
                    }
                    Err(e) => {
                        let _ = tx.blocking_send(ProgressUpdate::Error {
                            message: e.to_string(),
                        });
                        Err(e)
                    }
                }
            }
        })
        .await
        .map_err(|e| Error::Other(e.to_string()))?
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
