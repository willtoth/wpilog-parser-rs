//! Example showing low-level parsing API for custom processing.

use wpilog_parser::WpilogReader;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a reader
    let reader = WpilogReader::from_file("data.wpilog")?;

    // Get the low-level datalog reader
    let datalog_reader = reader.low_level_reader();

    println!("Using low-level API for custom parsing...");
    println!();

    let mut control_records = 0;
    let mut data_records = 0;
    let mut start_records = 0;
    let mut finish_records = 0;

    // Iterate through records manually
    for record_result in datalog_reader.records()? {
        let record = record_result?;

        if record.is_control() {
            control_records += 1;

            if record.is_start() {
                start_records += 1;
                let start_data = record.get_start_data()?;
                println!(
                    "START: entry={}, name='{}', type='{}'",
                    start_data.entry, start_data.name, start_data.type_name
                );
            } else if record.is_finish() {
                finish_records += 1;
                let entry_id = record.get_finish_entry()?;
                println!("FINISH: entry={}", entry_id);
            }
        } else {
            data_records += 1;

            // Custom processing based on data type
            // You can read the raw data here without the overhead
            // of converting to WideRow format
        }
    }

    println!();
    println!("═══════════════════════════════════════");
    println!("Record Statistics:");
    println!("═══════════════════════════════════════");
    println!("  Total control records: {}", control_records);
    println!("    Start records: {}", start_records);
    println!("    Finish records: {}", finish_records);
    println!("  Total data records: {}", data_records);
    println!("  Total: {}", control_records + data_records);

    Ok(())
}
