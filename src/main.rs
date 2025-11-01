//! Command-line interface for the WPILog parser.
//!
//! This binary provides a simple CLI for converting .wpilog files to Parquet format.

use anyhow::Result;
use clap::Parser;
use log::{info, LevelFilter};
use std::fs;
use std::path::Path;
use std::time::Instant;
use wpilog_parser::{ParquetWriter, WpilogReader};

#[derive(Parser, Debug)]
#[command(
    author,
    version,
    about = "Convert .wpilog files to Parquet format",
    long_about = "A high-performance parser for WPILib data log files (.wpilog) with output to Apache Parquet.\n\n\
                  Parquet files are columnar, compressed, and optimized for analytics queries."
)]
struct Args {
    /// Directory containing .wpilog files
    #[arg(value_name = "IN_DIR")]
    in_dir: String,

    /// Root output directory for converted Parquet files
    #[arg(short, long, value_name = "OUT_ROOT")]
    out_root: String,

    /// Number of rows per Parquet file chunk
    #[arg(long, default_value = "50000")]
    chunk_size: usize,
}

fn convert_one_file(input_file: &Path, output_dir: &Path, chunk_size: usize) -> Result<()> {
    let file_name = input_file.to_string_lossy();
    info!("📄 Processing: {}", file_name);

    let start_time = Instant::now();

    // Read the WPILog file
    let reader = WpilogReader::from_file(input_file)?;

    info!("   ├─ Version: {:#06x}", reader.version());

    let extra_header = reader.extra_header();
    if !extra_header.is_empty() {
        info!("   ├─ Extra header: {}", extra_header);
    }

    let t0 = Instant::now();
    let (records, formatter) = reader.read_all_with_metadata()?;
    info!(
        "   ├─ Read {} records in {:.2?}",
        records.len(),
        t0.elapsed()
    );
    info!(
        "   ├─ Found {} unique metrics",
        formatter.metrics_names.len()
    );

    // Write to Parquet
    let t1 = Instant::now();
    let stats = ParquetWriter::new(output_dir)
        .chunk_size(chunk_size)
        .write_with_stats(&records)?;

    info!("   ├─ Wrote Parquet in {:.2?}", t1.elapsed());
    info!("   ├─ {}", stats.summary());
    info!("   └─ ✓ Total time: {:.2?}\n", start_time.elapsed());

    Ok(())
}

fn main() -> Result<()> {
    // Initialize logger
    env_logger::Builder::new()
        .filter_level(LevelFilter::Info)
        .format_timestamp(None)
        .init();

    let args = Args::parse();

    let in_path = Path::new(&args.in_dir);
    let out_path = Path::new(&args.out_root);

    if !in_path.is_dir() {
        anyhow::bail!("'{}' is not a valid directory", args.in_dir);
    }

    // Find all .wpilog files
    let wpilog_files: Vec<_> = fs::read_dir(in_path)?
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.path().extension().and_then(|ext| ext.to_str()) == Some("wpilog"))
        .collect();

    if wpilog_files.is_empty() {
        info!("No .wpilog files found in {}", args.in_dir);
        return Ok(());
    }

    info!("");
    info!("╔════════════════════════════════════════════╗");
    info!("║       WPILog → Parquet Converter           ║");
    info!("╚════════════════════════════════════════════╝");
    info!("");
    info!(
        "📂 Found {} .wpilog file(s) in {}",
        wpilog_files.len(),
        args.in_dir
    );
    info!("📁 Output directory: {}", args.out_root);
    info!("📊 Chunk size: {} rows per file", args.chunk_size);
    info!("");

    let total_start = Instant::now();

    // Process each file
    for (idx, entry) in wpilog_files.iter().enumerate() {
        let input_file = entry.path();
        let file_name = input_file
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("unknown");

        info!("[{}/{}]", idx + 1, wpilog_files.len());

        // Create output directory for this file
        let output_dir = out_path.join(format!("filename={}", file_name));
        fs::create_dir_all(&output_dir)?;

        // Convert the file
        if let Err(e) = convert_one_file(&input_file, &output_dir, args.chunk_size) {
            log::error!("   └─ ✗ Error: {}", e);
            log::error!("");
            continue;
        }
    }

    info!("═══════════════════════════════════════════");
    info!("🏁 All files processed in {:.2?}", total_start.elapsed());
    info!("");

    Ok(())
}
