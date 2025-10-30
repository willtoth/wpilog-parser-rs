//! High-level API for writing parsed WPILog data to various formats.

use crate::error::{Error, Result};
use crate::formats::parquet::ParquetFormatter;
use crate::models::WideRow;
use crate::progress::ProgressUpdate;
use std::path::Path;
use std::sync::mpsc;

#[cfg(feature = "tokio-runtime")]
use tokio::sync::mpsc as tokio_mpsc;

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

    /// Write records to Parquet with progress reporting through a blocking channel.
    ///
    /// This method sends progress updates through the provided `std::sync::mpsc`
    /// channel as each chunk is written. This is ideal for non-async contexts or
    /// when you don't want tokio as a dependency.
    ///
    /// # Arguments
    ///
    /// * `records` - The WPILog records to write
    /// * `tx` - Channel sender for progress updates
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
    /// use std::thread;
    ///
    /// let reader = WpilogReader::from_file("data.wpilog")?;
    /// let records = reader.read_all()?;
    ///
    /// let (tx, rx) = std::sync::mpsc::channel();
    ///
    /// // Spawn thread to handle progress
    /// let progress_thread = thread::spawn(move || {
    ///     while let Ok(update) = rx.recv() {
    ///         match update {
    ///             wpilog_parser::ProgressUpdate::Progress { percent, .. } => {
    ///                 println!("Write progress: {:.1}%", percent);
    ///             }
    ///             wpilog_parser::ProgressUpdate::Complete { .. } => {
    ///                 println!("Write complete!");
    ///             }
    ///             _ => {}
    ///         }
    ///     }
    /// });
    ///
    /// let writer = ParquetWriter::new("./output").chunk_size(50_000);
    /// writer.write_with_progress(&records, tx)?;
    ///
    /// progress_thread.join().ok();
    /// # Ok::<(), wpilog_parser::Error>(())
    /// ```
    pub fn write_with_progress(
        self,
        records: &[WideRow],
        tx: mpsc::Sender<ProgressUpdate>,
    ) -> Result<WriteStats> {
        let num_records = records.len();
        let num_chunks = (num_records + self.chunk_size - 1) / self.chunk_size;
        let chunk_size = self.chunk_size;

        let formatter = ParquetFormatter::new(self.output_directory, self.chunk_size);

        formatter
            .convert_with_progress(records, tx)
            .map_err(|e| Error::OutputError(e.to_string()))?;

        Ok(WriteStats {
            num_records,
            num_chunks,
            chunk_size,
        })
    }

    /// Write records asynchronously with progress reporting using tokio.
    ///
    /// This method requires the `tokio-runtime` feature and spawns a blocking task
    /// to write Parquet files while sending progress updates through the returned
    /// channel. This is ideal for UI integration with async runtimes where you don't
    /// want to block the async runtime.
    ///
    /// # Arguments
    ///
    /// * `records` - The WPILog records to write
    ///
    /// # Returns
    ///
    /// A tuple of (future_result, progress_receiver) where:
    /// - `future_result` is a future that yields WriteStats
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
    /// use wpilog_parser::{WpilogReader, ParquetWriter};
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let reader = WpilogReader::from_file("data.wpilog")?;
    ///     let records = reader.read_all()?;
    ///
    ///     let writer = ParquetWriter::new("./output");
    ///     let (result, mut progress_rx) = writer.write_with_progress_async(&records);
    ///
    ///     // Spawn a task to handle progress updates
    ///     tokio::spawn(async move {
    ///         while let Some(update) = progress_rx.recv().await {
    ///             match update {
    ///                 wpilog_parser::ProgressUpdate::Progress { percent, .. } => {
    ///                     println!("Write progress: {:.1}%", percent);
    ///                 }
    ///                 _ => {}
    ///             }
    ///         }
    ///     });
    ///
    ///     let stats = result.await?;
    ///     println!("{}", stats.summary());
    ///     Ok(())
    /// }
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # }
    /// ```
    #[cfg(feature = "tokio-runtime")]
    pub fn write_with_progress_async(
        self,
        records: &[WideRow],
    ) -> (
        impl std::future::Future<Output = Result<WriteStats>>,
        tokio_mpsc::Receiver<ProgressUpdate>,
    ) {
        let (tx, rx) = tokio_mpsc::channel(64);
        let output_dir = self.output_directory.clone();
        let records = records.to_vec(); // Clone records for the blocking task

        let future = async move {
            tokio::task::spawn_blocking({
                let tx = tx.clone();
                let records = records.clone();
                move || {
                    let writer = ParquetWriter::new(output_dir);
                    // Need to convert tokio_mpsc sender to std::sync::mpsc for the blocking task
                    // For now, we'll use the sync progress method and wrap it
                    // This is a known limitation - we can't use tokio channels in blocking context
                    let (sync_tx, sync_rx) = std::sync::mpsc::channel();

                    let result = writer.write_with_progress(&records, sync_tx);

                    // Forward progress updates from sync channel to tokio channel
                    while let Ok(update) = sync_rx.recv() {
                        let _ = tx.blocking_send(update);
                    }

                    result
                }
            })
            .await
            .map_err(|e| Error::Other(e.to_string()))?
        };

        (future, rx)
    }

    /// Write records asynchronously with progress reporting using a tokio channel.
    ///
    /// This variant allows you to provide your own tokio progress sender for more
    /// control over how progress updates are handled. This requires the `tokio-runtime`
    /// feature to be enabled.
    ///
    /// # Arguments
    ///
    /// * `records` - The WPILog records to write
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
    /// use wpilog_parser::{WpilogReader, ParquetWriter};
    /// use tokio::sync::mpsc;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let reader = WpilogReader::from_file("data.wpilog")?;
    ///     let records = reader.read_all()?;
    ///
    ///     let (tx, mut rx) = mpsc::channel(64);
    ///
    ///     // Spawn task to handle progress
    ///     let progress_task = tokio::spawn(async move {
    ///         while let Some(update) = rx.recv().await {
    ///             match update {
    ///                 wpilog_parser::ProgressUpdate::Progress { percent, .. } => {
    ///                     println!("Write progress: {:.1}%", percent);
    ///                 }
    ///                 wpilog_parser::ProgressUpdate::Complete { .. } => {
    ///                     println!("Write complete!");
    ///                 }
    ///                 _ => {}
    ///             }
    ///         }
    ///     });
    ///
    ///     let writer = ParquetWriter::new("./output").chunk_size(50_000);
    ///     writer.write_with_progress_channel(&records, tx).await?;
    ///
    ///     progress_task.await?;
    ///     Ok(())
    /// }
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # }
    /// ```
    #[cfg(feature = "tokio-runtime")]
    pub async fn write_with_progress_channel(
        self,
        records: &[WideRow],
        tx: tokio_mpsc::Sender<ProgressUpdate>,
    ) -> Result<WriteStats> {
        let output_dir = self.output_directory.clone();
        let records = records.to_vec();
        let chunk_size = self.chunk_size;

        tokio::task::spawn_blocking({
            let tx = tx.clone();
            let records = records.clone();
            move || {
                let writer = ParquetWriter::new(output_dir);
                // Convert tokio channel to sync for the blocking task
                let (sync_tx, sync_rx) = std::sync::mpsc::channel();

                let result = writer.write_with_progress(&records, sync_tx);

                // Forward progress updates from sync channel to tokio channel
                while let Ok(update) = sync_rx.recv() {
                    let _ = tx.blocking_send(update);
                }

                result
            }
        })
        .await
        .map_err(|e| Error::Other(e.to_string()))?
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
