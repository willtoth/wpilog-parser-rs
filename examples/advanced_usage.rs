//! Advanced example showing metadata access and low-level parsing.

use wpilog_parser::WpilogReader;

fn main() -> Result<(), wpilog_parser::Error> {
    // Read with metadata
    let reader = WpilogReader::from_file("data.wpilog")?;
    let (records, formatter) = reader.read_all_with_metadata()?;

    println!("═══════════════════════════════════════");
    println!("WPILog File Analysis");
    println!("═══════════════════════════════════════");
    println!();

    println!("📊 Statistics:");
    println!("  Total records: {}", records.len());
    println!("  Unique metrics: {}", formatter.metrics_names.len());
    println!("  Struct schemas: {}", formatter.struct_schemas.len());
    println!();

    println!("📝 Metric Names:");
    let mut names: Vec<_> = formatter.metrics_names.iter().collect();
    names.sort();
    for (i, name) in names.iter().take(10).enumerate() {
        println!("  {}. {}", i + 1, name);
    }
    if names.len() > 10 {
        println!("  ... and {} more", names.len() - 10);
    }
    println!();

    println!("🏗️  Struct Schemas:");
    for schema in &formatter.struct_schemas {
        println!("  {}: {} fields", schema.name, schema.columns.len());
        for col in &schema.columns {
            println!("    - {} ({})", col.name, col.type_name);
        }
    }
    println!();

    println!("📈 First 5 Records:");
    for (i, record) in records.iter().take(5).enumerate() {
        println!(
            "  {}: t={:.3}s, entry={}, type={}",
            i + 1,
            record.timestamp,
            record.entry,
            record.type_name
        );
    }

    Ok(())
}
