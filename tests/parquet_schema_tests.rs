mod common;

use common::WpilogBuilder;
use std::fs::File;
use std::io::Write;
use tempfile::tempdir;
use wpilog_parser::formatter::Formatter;
use wpilog_parser::formats::parquet::ParquetFormatter;
use wpilog_parser::models::OutputFormat;

#[test]
fn test_double_array_schema_type() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("test.wpilog");

    let data = WpilogBuilder::new()
        .start_record(1_000_000, 1, "/velocities", "double[]", "")
        .double_array_record(1, 1_100_000, &[1.1, 2.2, 3.3])
        .double_array_record(1, 1_200_000, &[4.4, 5.5, 6.6])
        .build();

    File::create(&file_path)
        .unwrap()
        .write_all(&data)
        .unwrap();

    let mut formatter = Formatter::new(
        file_path.to_str().unwrap().to_string(),
        dir.path().to_str().unwrap().to_string(),
        OutputFormat::Wide,
    );

    formatter.read_wpilog(true).unwrap();
    let rows = formatter.read_wpilog(false).unwrap();

    assert_eq!(rows.len(), 2);

    // Write to parquet
    let output_dir = dir.path().join("output");
    let parquet_formatter = ParquetFormatter::new(output_dir.to_str().unwrap().to_string(), 50_000);
    parquet_formatter.convert(&rows).unwrap();

    // Verify the parquet file was created
    let parquet_file = output_dir.join("file_part000.parquet");
    assert!(parquet_file.exists());

    // Read back and verify schema using parquet crate
    use parquet::file::reader::{FileReader, SerializedFileReader};
    let file = File::open(parquet_file).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let schema = reader.metadata().file_metadata().schema();

    // Find the /velocities column
    let velocities_field = schema
        .get_fields()
        .iter()
        .find(|f| f.name() == "/velocities")
        .expect("Should have /velocities column");

    // Check it's a List type
    assert!(
        velocities_field.is_primitive() == false,
        "Expected List type, but got primitive"
    );

    println!("Schema: {:?}", schema);
    println!("Velocities field: {:?}", velocities_field);
}

#[test]
fn test_int64_array_schema_type() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("test.wpilog");

    let data = WpilogBuilder::new()
        .start_record(1_000_000, 1, "/counts", "int64[]", "")
        .int64_array_record(1, 1_100_000, &[1, 2, 3])
        .int64_array_record(1, 1_200_000, &[4, 5, 6])
        .build();

    File::create(&file_path)
        .unwrap()
        .write_all(&data)
        .unwrap();

    let mut formatter = Formatter::new(
        file_path.to_str().unwrap().to_string(),
        dir.path().to_str().unwrap().to_string(),
        OutputFormat::Wide,
    );

    formatter.read_wpilog(true).unwrap();
    let rows = formatter.read_wpilog(false).unwrap();

    // Write to parquet
    let output_dir = dir.path().join("output");
    let parquet_formatter = ParquetFormatter::new(output_dir.to_str().unwrap().to_string(), 50_000);
    parquet_formatter.convert(&rows).unwrap();

    // Verify the parquet file was created
    let parquet_file = output_dir.join("file_part000.parquet");
    assert!(parquet_file.exists());

    // Read back and verify schema
    use parquet::file::reader::{FileReader, SerializedFileReader};
    let file = File::open(parquet_file).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let schema = reader.metadata().file_metadata().schema();

    // Find the /counts column
    let counts_field = schema
        .get_fields()
        .iter()
        .find(|f| f.name() == "/counts")
        .expect("Should have /counts column");

    // Check it's a List type
    assert!(
        counts_field.is_primitive() == false,
        "Expected List type, but got primitive"
    );

    println!("Schema: {:?}", schema);
    println!("Counts field: {:?}", counts_field);
}

#[test]
fn test_boolean_array_schema_type() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("test.wpilog");

    let data = WpilogBuilder::new()
        .start_record(1_000_000, 1, "/flags", "boolean[]", "")
        .boolean_array_record(1, 1_100_000, &[true, false, true])
        .build();

    File::create(&file_path)
        .unwrap()
        .write_all(&data)
        .unwrap();

    let mut formatter = Formatter::new(
        file_path.to_str().unwrap().to_string(),
        dir.path().to_str().unwrap().to_string(),
        OutputFormat::Wide,
    );

    formatter.read_wpilog(true).unwrap();
    let rows = formatter.read_wpilog(false).unwrap();

    // Write to parquet
    let output_dir = dir.path().join("output");
    let parquet_formatter = ParquetFormatter::new(output_dir.to_str().unwrap().to_string(), 50_000);
    parquet_formatter.convert(&rows).unwrap();

    // Verify the parquet file exists
    let parquet_file = output_dir.join("file_part000.parquet");
    assert!(parquet_file.exists());
}

#[test]
fn test_string_array_schema_type() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("test.wpilog");

    let data = WpilogBuilder::new()
        .start_record(1_000_000, 1, "/labels", "string[]", "")
        .string_array_record(1, 1_100_000, &["a", "b", "c"])
        .build();

    File::create(&file_path)
        .unwrap()
        .write_all(&data)
        .unwrap();

    let mut formatter = Formatter::new(
        file_path.to_str().unwrap().to_string(),
        dir.path().to_str().unwrap().to_string(),
        OutputFormat::Wide,
    );

    formatter.read_wpilog(true).unwrap();
    let rows = formatter.read_wpilog(false).unwrap();

    // Write to parquet
    let output_dir = dir.path().join("output");
    let parquet_formatter = ParquetFormatter::new(output_dir.to_str().unwrap().to_string(), 50_000);
    parquet_formatter.convert(&rows).unwrap();

    // Verify the parquet file exists
    let parquet_file = output_dir.join("file_part000.parquet");
    assert!(parquet_file.exists());
}

#[test]
fn test_mixed_scalar_and_array_columns() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("test.wpilog");

    let data = WpilogBuilder::new()
        .start_record(1_000_000, 1, "/temperature", "double", "")
        .start_record(1_000_000, 2, "/velocities", "double[]", "")
        .start_record(1_000_000, 3, "/enabled", "boolean", "")
        .double_record(1, 1_100_000, 25.5)
        .double_array_record(2, 1_100_000, &[1.1, 2.2, 3.3])
        .boolean_record(3, 1_100_000, true)
        .build();

    File::create(&file_path)
        .unwrap()
        .write_all(&data)
        .unwrap();

    let mut formatter = Formatter::new(
        file_path.to_str().unwrap().to_string(),
        dir.path().to_str().unwrap().to_string(),
        OutputFormat::Wide,
    );

    formatter.read_wpilog(true).unwrap();
    let rows = formatter.read_wpilog(false).unwrap();

    // Write to parquet
    let output_dir = dir.path().join("output");
    let parquet_formatter = ParquetFormatter::new(output_dir.to_str().unwrap().to_string(), 50_000);
    parquet_formatter.convert(&rows).unwrap();

    // Verify the parquet file was created
    let parquet_file = output_dir.join("file_part000.parquet");
    assert!(parquet_file.exists());

    // Read back and verify schema
    use parquet::file::reader::{FileReader, SerializedFileReader};
    let file = File::open(parquet_file).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let schema = reader.metadata().file_metadata().schema();

    println!("Complete schema: {:?}", schema);

    // Temperature should be primitive (double)
    let temp_field = schema
        .get_fields()
        .iter()
        .find(|f| f.name() == "/temperature")
        .expect("Should have /temperature column");
    assert!(temp_field.is_primitive(), "Temperature should be primitive");

    // Velocities should be List
    let vel_field = schema
        .get_fields()
        .iter()
        .find(|f| f.name() == "/velocities")
        .expect("Should have /velocities column");
    assert!(!vel_field.is_primitive(), "Velocities should be List");

    // Enabled should be primitive (boolean)
    let enabled_field = schema
        .get_fields()
        .iter()
        .find(|f| f.name() == "/enabled")
        .expect("Should have /enabled column");
    assert!(enabled_field.is_primitive(), "Enabled should be primitive");
}
