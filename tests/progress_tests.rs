//! Integration tests for progress tracking functionality
use wpilog_parser::progress::{ProgressTracker, ProgressUpdate};
use tokio::sync::mpsc;

#[test]
fn test_progress_tracker_creation() {
    let tracker = ProgressTracker::new(1000);
    assert_eq!(tracker.total(), 1000);
    assert_eq!(tracker.processed(), 0);
    assert_eq!(tracker.percent(), 0.0);
}

#[test]
fn test_progress_tracker_increment() {
    let tracker = ProgressTracker::new(100);
    tracker.increment();
    assert_eq!(tracker.processed(), 1);
    assert_eq!(tracker.percent(), 1.0);

    tracker.increment_by(49);
    assert_eq!(tracker.processed(), 50);
    assert_eq!(tracker.percent(), 50.0);

    tracker.increment_by(50);
    assert_eq!(tracker.processed(), 100);
    assert_eq!(tracker.percent(), 100.0);
}

#[test]
fn test_progress_tracker_phase_changes() {
    let tracker = ProgressTracker::new(1000);
    assert_eq!(tracker.phase(), "Starting");
    assert_eq!(tracker.phase_count(), 0);

    tracker.set_phase("Phase 1");
    assert_eq!(tracker.phase(), "Phase 1");
    assert_eq!(tracker.phase_count(), 1);

    tracker.set_phase("Phase 2");
    assert_eq!(tracker.phase(), "Phase 2");
    assert_eq!(tracker.phase_count(), 2);
}

#[test]
fn test_progress_tracker_reset() {
    let tracker = ProgressTracker::new(100);
    tracker.increment_by(50);
    assert_eq!(tracker.processed(), 50);

    tracker.reset();
    assert_eq!(tracker.processed(), 0);
}

#[test]
fn test_progress_update_enum_variants() {
    // Test Started
    let started = ProgressUpdate::Started {
        phase: "Test Phase".to_string(),
        total: 1000,
    };
    match started {
        ProgressUpdate::Started { phase, total } => {
            assert_eq!(phase, "Test Phase");
            assert_eq!(total, 1000);
        }
        _ => panic!("Expected Started variant"),
    }

    // Test Progress
    let progress = ProgressUpdate::Progress {
        percent: 50.0,
        processed: 500,
        total: 1000,
        current_phase: "Processing".to_string(),
    };
    match progress {
        ProgressUpdate::Progress {
            percent,
            processed,
            total,
            current_phase,
        } => {
            assert_eq!(percent, 50.0);
            assert_eq!(processed, 500);
            assert_eq!(total, 1000);
            assert_eq!(current_phase, "Processing");
        }
        _ => panic!("Expected Progress variant"),
    }

    // Test Complete
    let complete = ProgressUpdate::Complete {
        total_processed: 1000,
    };
    match complete {
        ProgressUpdate::Complete { total_processed } => {
            assert_eq!(total_processed, 1000);
        }
        _ => panic!("Expected Complete variant"),
    }

    // Test Error
    let error = ProgressUpdate::Error {
        message: "Test error".to_string(),
    };
    match error {
        ProgressUpdate::Error { message } => {
            assert_eq!(message, "Test error");
        }
        _ => panic!("Expected Error variant"),
    }
}

#[test]
fn test_progress_tracker_create_update() {
    let tracker = ProgressTracker::new(1000);
    tracker.set_phase("Testing");
    tracker.increment_by(250);

    let update = tracker.create_update();
    match update {
        ProgressUpdate::Progress {
            percent,
            processed,
            total,
            current_phase,
        } => {
            assert_eq!(percent, 25.0);
            assert_eq!(processed, 250);
            assert_eq!(total, 1000);
            assert_eq!(current_phase, "Testing");
        }
        _ => panic!("Expected Progress variant"),
    }
}

#[test]
fn test_progress_tracker_is_complete() {
    let tracker = ProgressTracker::new(100);
    assert!(!tracker.is_complete());

    tracker.increment_by(100);
    assert!(tracker.is_complete());
}

#[test]
fn test_progress_tracker_zero_total() {
    let tracker = ProgressTracker::new(0);
    assert_eq!(tracker.percent(), 0.0);
    assert!(!tracker.is_complete());
}

#[tokio::test]
async fn test_progress_update_enum_clone_and_debug() {
    let update = ProgressUpdate::Progress {
        percent: 50.0,
        processed: 500,
        total: 1000,
        current_phase: "Testing".to_string(),
    };

    // Test Clone
    let cloned = update.clone();
    assert_eq!(std::mem::discriminant(&update), std::mem::discriminant(&cloned));

    // Test Debug
    let debug_str = format!("{:?}", update);
    assert!(debug_str.contains("Progress"));
    assert!(debug_str.contains("50"));
}

#[test]
fn test_progress_tracker_multiple_threads() {
    use std::sync::Arc;
    use std::thread;

    let tracker = Arc::new(ProgressTracker::new(10000));

    let mut handles = vec![];

    for _ in 0..10 {
        let t = tracker.clone();
        let handle = thread::spawn(move || {
            for _ in 0..1000 {
                t.increment();
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    assert_eq!(tracker.processed(), 10000);
    assert_eq!(tracker.percent(), 100.0);
}

#[tokio::test]
async fn test_mpsc_channel_with_progress_updates() {
    let (tx, mut rx) = mpsc::channel(64);

    // Spawn a task to send progress updates
    tokio::spawn(async move {
        for i in 0..5 {
            let _ = tx
                .send(ProgressUpdate::Progress {
                    percent: (i as f32 / 5.0) * 100.0,
                    processed: i,
                    total: 5,
                    current_phase: format!("Step {}", i),
                })
                .await;
        }
    });

    // Receive progress updates
    let mut updates = vec![];
    while let Some(update) = rx.recv().await {
        updates.push(update);
    }

    assert_eq!(updates.len(), 5);
    for (i, update) in updates.iter().enumerate() {
        match update {
            ProgressUpdate::Progress { percent, .. } => {
                assert_eq!(*percent, (i as f32 / 5.0) * 100.0);
            }
            _ => panic!("Expected Progress variant"),
        }
    }
}

#[test]
fn test_progress_update_blocking_send() {
    let (tx, mut rx) = mpsc::channel(64);

    let update = ProgressUpdate::Progress {
        percent: 50.0,
        processed: 500,
        total: 1000,
        current_phase: "Testing".to_string(),
    };

    // This should work from a blocking context
    let _ = tx.blocking_send(update);

    // Receive the update
    let received = rx.blocking_recv();
    assert!(received.is_some());
}
