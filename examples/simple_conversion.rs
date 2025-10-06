//! Simple example of converting a WPILog file to Parquet.

use wpilog_parser::{ParquetWriter, WpilogReader};

fn main() -> Result<(), wpilog_parser::Error> {
    // Read the WPILog file
    let reader = WpilogReader::from_file("data.wpilog")?;

    println!("File version: {:#06x}", reader.version());

    // Read all records
    let records = reader.read_all()?;

    println!("Read {} records", records.len());

    // Write to Parquet
    ParquetWriter::new("output")
        .chunk_size(100_000)
        .write(&records)?;

    println!("Wrote Parquet files to ./output");

    Ok(())
}
