//! Progress tracking infrastructure for read and write operations.
//!
//! This module provides types for reporting progress from long-running operations
//! to UI threads or other progress consumers. It supports both synchronous and
//! asynchronous APIs depending on your needs.
//!
//! # Overview
//!
//! The progress tracking system consists of two main components:
//!
//! - **[`ProgressUpdate`]**: An enum that represents different types of progress events
//! - **[`ProgressTracker`]**: A thread-safe tracker for monitoring operation progress
//!
//! # ProgressUpdate Variants
//!
//! - **Started**: Indicates an operation has begun with total item count
//! - **Progress**: Reports current progress with percentage, count, and phase name
//! - **PhaseChanged**: Notifies of a transition between operation phases
//! - **Complete**: Signals successful completion with total items processed
//! - **Error**: Reports an error during operation
//!
//! # Usage with Synchronous Channels (No Dependencies)
//!
//! Progress updates work with `std::sync::mpsc` channels for zero-dependency progress tracking:
//!
//! ```no_run
//! use wpilog_parser::WpilogReader;
//! use std::sync::mpsc;
//! use std::thread;
//!
//! let reader = WpilogReader::from_file("data.wpilog")?;
//! let (tx, rx) = mpsc::channel();
//!
//! // Spawn thread to do the work
//! let handle = thread::spawn(move || {
//!     reader.read_all_with_progress(tx)
//! });
//!
//! // Monitor progress in main thread
//! for update in rx {
//!     match update {
//!         wpilog_parser::ProgressUpdate::Progress { percent, current_phase, .. } => {
//!             eprintln!("{}: {:.1}%", current_phase, percent);
//!         }
//!         wpilog_parser::ProgressUpdate::Complete { total_processed } => {
//!             eprintln!("Done! Processed {} items", total_processed);
//!             break;
//!         }
//!         _ => {}
//!     }
//! }
//!
//! let records = handle.join().unwrap()?;
//! eprintln!("Read {} records", records.len());
//! # Ok::<(), wpilog_parser::Error>(())
//! ```
//!
//! # Usage with Async Channels (Requires `tokio-runtime` feature)
//!
//! Progress updates also work with `tokio::sync::mpsc` channels for async contexts:
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
//!     let (result, mut progress_rx) = reader.read_all_with_progress_async();
//!
//!     // Spawn UI update task
//!     tokio::spawn(async move {
//!         while let Some(update) = progress_rx.recv().await {
//!             match update {
//!                 wpilog_parser::ProgressUpdate::Progress { percent, current_phase, .. } => {
//!                     eprintln!("{}: {:.1}%", current_phase, percent);
//!                 }
//!                 wpilog_parser::ProgressUpdate::Complete { total_processed } => {
//!                     eprintln!("Done! Processed {} items", total_processed);
//!                 }
//!                 _ => {}
//!             }
//!         }
//!     });
//!
//!     let records = result.await?;
//!     eprintln!("Read {} records", records.len());
//!     Ok(())
//! }
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! # }
//! ```
//!
//! # Thread-Safe Progress Tracking
//!
//! The `[`ProgressTracker`] uses atomic operations for thread-safe progress updates
//! without requiring locks:
//!
//! ```no_run
//! use wpilog_parser::ProgressTracker;
//! use std::sync::Arc;
//! use std::thread;
//!
//! let tracker = Arc::new(ProgressTracker::new(10000));
//!
//! let t = tracker.clone();
//! thread::spawn(move || {
//!     for i in 0..10000 {
//!         t.increment();
//!         if i % 1000 == 0 {
//!             println!("Progress: {:.1}%", t.percent());
//!         }
//!     }
//! });
//! # drop(tracker);
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// Progress update sent through a channel to UI or progress consumers.
///
/// # Examples
///
/// ```no_run
/// use wpilog_parser::progress::ProgressUpdate;
///
/// let update = ProgressUpdate::Progress {
///     percent: 45.5,
///     processed: 45000,
///     total: 100000,
///     current_phase: "Reading records".to_string(),
/// };
/// ```
#[derive(Debug, Clone)]
pub enum ProgressUpdate {
    /// Operation started.
    ///
    /// # Fields
    ///
    /// * `phase` - Descriptive name of the current phase
    /// * `total` - Total items/bytes to process
    Started { phase: String, total: u64 },

    /// Progress update during processing.
    ///
    /// # Fields
    ///
    /// * `percent` - Percentage complete (0.0 to 100.0)
    /// * `processed` - Number of items/bytes processed so far
    /// * `total` - Total items/bytes to process
    /// * `current_phase` - Descriptive name of current phase
    Progress {
        percent: f32,
        processed: u64,
        total: u64,
        current_phase: String,
    },

    /// Phase transition in multi-phase operations.
    ///
    /// # Fields
    ///
    /// * `phase` - Name of the new phase
    /// * `percent` - Overall completion percentage
    PhaseChanged { phase: String, percent: f32 },

    /// Operation completed successfully.
    Complete { total_processed: u64 },

    /// Operation encountered an error.
    ///
    /// # Fields
    ///
    /// * `message` - Error description
    Error { message: String },
}

/// Thread-safe progress tracker for long-running operations.
///
/// This tracker uses atomic operations to avoid the need for locks,
/// making it suitable for multi-threaded environments.
///
/// # Examples
///
/// ```no_run
/// use wpilog_parser::progress::ProgressTracker;
///
/// let tracker = ProgressTracker::new(100_000);
/// tracker.set_total(100_000);
///
/// for i in 0..100_000 {
///     // Do work...
///     tracker.increment();
///
///     if i % 1000 == 0 {
///         let percent = tracker.percent();
///         println!("Progress: {:.1}%", percent);
///     }
/// }
/// ```
#[derive(Debug)]
pub struct ProgressTracker {
    total: AtomicU64,
    processed: AtomicU64,
    phase: Arc<Mutex<String>>,
    phase_count: AtomicU64,
}

impl ProgressTracker {
    /// Create a new progress tracker with a known total count.
    ///
    /// # Arguments
    ///
    /// * `total` - Total number of items/bytes to process
    pub fn new(total: u64) -> Self {
        Self {
            total: AtomicU64::new(total),
            processed: AtomicU64::new(0),
            phase: Arc::new(Mutex::new("Starting".to_string())),
            phase_count: AtomicU64::new(0),
        }
    }

    /// Create a tracker with unknown total (will be set later).
    pub fn new_unknown() -> Self {
        Self::new(0)
    }

    /// Set or update the total count.
    pub fn set_total(&self, total: u64) {
        self.total.store(total, Ordering::Relaxed);
    }

    /// Get the total count.
    pub fn total(&self) -> u64 {
        self.total.load(Ordering::Relaxed)
    }

    /// Increment processed count by 1.
    pub fn increment(&self) {
        self.increment_by(1);
    }

    /// Increment processed count by a specific amount.
    pub fn increment_by(&self, amount: u64) {
        self.processed.fetch_add(amount, Ordering::Relaxed);
    }

    /// Get the current processed count.
    pub fn processed(&self) -> u64 {
        self.processed.load(Ordering::Relaxed)
    }

    /// Reset the processed count to zero.
    pub fn reset(&self) {
        self.processed.store(0, Ordering::Relaxed);
    }

    /// Get completion percentage (0.0 to 100.0).
    pub fn percent(&self) -> f32 {
        let total = self.total.load(Ordering::Relaxed);
        let processed = self.processed.load(Ordering::Relaxed);

        if total == 0 {
            0.0
        } else {
            (processed as f32 / total as f32) * 100.0
        }
    }

    /// Set the current phase name.
    pub fn set_phase(&self, phase: impl Into<String>) {
        if let Ok(mut p) = self.phase.lock() {
            *p = phase.into();
            self.phase_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Get the current phase name.
    pub fn phase(&self) -> String {
        self.phase
            .lock()
            .map(|p| p.clone())
            .unwrap_or_else(|_| "Unknown".to_string())
    }

    /// Get the number of times the phase has changed.
    pub fn phase_count(&self) -> u64 {
        self.phase_count.load(Ordering::Relaxed)
    }

    /// Create a progress update based on current state.
    pub fn create_update(&self) -> ProgressUpdate {
        ProgressUpdate::Progress {
            percent: self.percent(),
            processed: self.processed(),
            total: self.total(),
            current_phase: self.phase(),
        }
    }

    /// Check if work is complete (processed >= total).
    pub fn is_complete(&self) -> bool {
        let total = self.total.load(Ordering::Relaxed);
        let processed = self.processed.load(Ordering::Relaxed);
        total > 0 && processed >= total
    }
}

/// Type alias for sending progress updates in synchronous contexts.
///
/// This is a standard library `mpsc::Sender` that sends [`ProgressUpdate`] messages.
/// Pass this to operations that support progress tracking.
///
/// # Examples
///
/// ```no_run
/// use wpilog_parser::{WpilogReader, ProgressSender};
/// use std::sync::mpsc;
/// use std::thread;
///
/// let reader = WpilogReader::from_file("data.wpilog")?;
/// let (tx, rx) = mpsc::channel();
///
/// // Spawn thread to do the work
/// let handle = thread::spawn(move || {
///     reader.read_all_with_progress(tx)
/// });
///
/// // Monitor progress in main thread
/// for update in rx {
///     match update {
///         wpilog_parser::ProgressUpdate::Progress { percent, .. } => {
///             println!("Progress: {:.1}%", percent);
///         }
///         wpilog_parser::ProgressUpdate::Complete { .. } => {
///             println!("Done!");
///             break;
///         }
///         _ => {}
///     }
/// }
///
/// let records = handle.join().unwrap()?;
/// println!("Read {} records", records.len());
/// # Ok::<(), wpilog_parser::Error>(())
/// ```
pub type ProgressSender = std::sync::mpsc::Sender<ProgressUpdate>;

/// Type alias for receiving progress updates in synchronous contexts.
///
/// This is a standard library `mpsc::Receiver` that receives [`ProgressUpdate`] messages.
/// Use this to monitor progress from long-running operations without requiring async.
pub type ProgressReceiver = std::sync::mpsc::Receiver<ProgressUpdate>;

#[cfg(feature = "tokio-runtime")]
/// Type alias for receiving progress updates in async contexts.
///
/// This is a `tokio::sync::mpsc::Receiver` that receives [`ProgressUpdate`] messages.
/// Use this to monitor progress from long-running operations in async/await code.
///
/// This type is only available when the `tokio-runtime` feature is enabled.
///
/// # Examples
///
/// ```no_run
/// # #[cfg(feature = "tokio-runtime")]
/// # {
/// use wpilog_parser::WpilogReader;
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let reader = WpilogReader::from_file("data.wpilog")?;
///     let (result, mut progress_rx) = reader.read_all_with_progress_async();
///
///     // Spawn task to monitor progress
///     tokio::spawn(async move {
///         while let Some(update) = progress_rx.recv().await {
///             match update {
///                 wpilog_parser::ProgressUpdate::Progress { percent, .. } => {
///                     println!("Progress: {:.1}%", percent);
///                 }
///                 _ => {}
///             }
///         }
///     });
///
///     let records = result.await?;
///     Ok(())
/// }
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// # }
/// ```
pub type AsyncProgressReceiver = tokio::sync::mpsc::Receiver<ProgressUpdate>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_progress_tracker_creation() {
        let tracker = ProgressTracker::new(1000);
        assert_eq!(tracker.total(), 1000);
        assert_eq!(tracker.processed(), 0);
        assert_eq!(tracker.percent(), 0.0);
    }

    #[test]
    fn test_progress_increment() {
        let tracker = ProgressTracker::new(1000);
        tracker.increment();
        assert_eq!(tracker.processed(), 1);

        tracker.increment_by(99);
        assert_eq!(tracker.processed(), 100);
    }

    #[test]
    fn test_progress_percent() {
        let tracker = ProgressTracker::new(100);
        assert_eq!(tracker.percent(), 0.0);

        tracker.increment_by(25);
        assert_eq!(tracker.percent(), 25.0);

        tracker.increment_by(50);
        assert_eq!(tracker.percent(), 75.0);

        tracker.increment_by(25);
        assert_eq!(tracker.percent(), 100.0);
    }

    #[test]
    fn test_progress_phase() {
        let tracker = ProgressTracker::new(1000);
        assert_eq!(tracker.phase(), "Starting");

        tracker.set_phase("Processing");
        assert_eq!(tracker.phase(), "Processing");
        assert_eq!(tracker.phase_count(), 1);

        tracker.set_phase("Finalizing");
        assert_eq!(tracker.phase(), "Finalizing");
        assert_eq!(tracker.phase_count(), 2);
    }

    #[test]
    fn test_progress_reset() {
        let tracker = ProgressTracker::new(1000);
        tracker.increment_by(500);
        assert_eq!(tracker.processed(), 500);

        tracker.reset();
        assert_eq!(tracker.processed(), 0);
    }

    #[test]
    fn test_progress_update_enum() {
        let update = ProgressUpdate::Progress {
            percent: 50.0,
            processed: 500,
            total: 1000,
            current_phase: "Reading".to_string(),
        };

        match update {
            ProgressUpdate::Progress { percent, .. } => {
                assert_eq!(percent, 50.0);
            }
            _ => panic!("Expected Progress variant"),
        }
    }
}
