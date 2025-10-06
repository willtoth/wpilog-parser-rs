mod common;

use byteorder::{LittleEndian, WriteBytesExt};
use common::WpilogBuilder;
use std::fs::File;
use std::io::Write;
use tempfile::tempdir;
use wpilog_parser::formatter::Formatter;
use wpilog_parser::models::OutputFormat;

// ============================================================================
// FULL PARSING PIPELINE TESTS
// ============================================================================

#[test]
fn test_parse_single_double_value() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("test.wpilog");

    let data = WpilogBuilder::new()
        .start_record(1_000_000, 1, "/sensor/temperature", "double", "")
        .double_record(1, 1_100_000, 25.5)
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

    // First pass: infer schema
    formatter.read_wpilog(true).unwrap();

    // Second pass: read data
    let rows = formatter.read_wpilog(false).unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].entry, 1);
    assert_eq!(rows[0].type_name, "double");
    assert_eq!((rows[0].timestamp * 1_000_000.0) as u64, 1_100_000);

    let value = rows[0].data.get("/sensor/temperature").unwrap();
    assert_eq!(value.as_f64().unwrap(), 25.5);
}

#[test]
fn test_parse_multiple_entries() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("test.wpilog");

    let data = WpilogBuilder::new()
        .start_record(1_000_000, 1, "/sensor/temperature", "double", "")
        .start_record(1_000_000, 2, "/sensor/pressure", "double", "")
        .start_record(1_000_000, 3, "/sensor/enabled", "boolean", "")
        .double_record(1, 1_100_000, 25.5)
        .double_record(2, 1_100_000, 101.3)
        .boolean_record(3, 1_100_000, true)
        .double_record(1, 1_200_000, 26.0)
        .double_record(2, 1_200_000, 101.5)
        .boolean_record(3, 1_200_000, false)
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

    assert_eq!(rows.len(), 6);

    // Check first timestamp
    assert_eq!(rows[0].data.get("/sensor/temperature").unwrap().as_f64().unwrap(), 25.5);
    assert_eq!(rows[1].data.get("/sensor/pressure").unwrap().as_f64().unwrap(), 101.3);
    assert_eq!(rows[2].data.get("/sensor/enabled").unwrap().as_bool().unwrap(), true);

    // Check second timestamp
    assert_eq!(rows[3].data.get("/sensor/temperature").unwrap().as_f64().unwrap(), 26.0);
    assert_eq!(rows[4].data.get("/sensor/pressure").unwrap().as_f64().unwrap(), 101.5);
    assert_eq!(rows[5].data.get("/sensor/enabled").unwrap().as_bool().unwrap(), false);
}

#[test]
fn test_parse_all_scalar_types() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("test.wpilog");

    let data = WpilogBuilder::new()
        .start_record(1_000_000, 1, "/bool", "boolean", "")
        .start_record(1_000_000, 2, "/int", "int64", "")
        .start_record(1_000_000, 3, "/float", "float", "")
        .start_record(1_000_000, 4, "/double", "double", "")
        .start_record(1_000_000, 5, "/string", "string", "")
        .boolean_record(1, 1_100_000, true)
        .int64_record(2, 1_100_000, 42)
        .float_record(3, 1_100_000, 3.14)
        .double_record(4, 1_100_000, 2.71828)
        .string_record(5, 1_100_000, "test")
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

    assert_eq!(rows.len(), 5);
    assert_eq!(rows[0].data.get("/bool").unwrap().as_bool().unwrap(), true);
    assert_eq!(rows[1].data.get("/int").unwrap().as_i64().unwrap(), 42);
    assert!((rows[2].data.get("/float").unwrap().as_f64().unwrap() - 3.14).abs() < 0.01);
    assert!((rows[3].data.get("/double").unwrap().as_f64().unwrap() - 2.71828).abs() < 0.00001);
    assert_eq!(rows[4].data.get("/string").unwrap().as_str().unwrap(), "test");
}

#[test]
fn test_parse_arrays() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("test.wpilog");

    let data = WpilogBuilder::new()
        .start_record(1_000_000, 1, "/bool_array", "boolean[]", "")
        .start_record(1_000_000, 2, "/int_array", "int64[]", "")
        .start_record(1_000_000, 3, "/float_array", "float[]", "")
        .start_record(1_000_000, 4, "/double_array", "double[]", "")
        .start_record(1_000_000, 5, "/string_array", "string[]", "")
        .boolean_array_record(1, 1_100_000, &[true, false, true])
        .int64_array_record(2, 1_100_000, &[1, 2, 3])
        .float_array_record(3, 1_100_000, &[1.1, 2.2, 3.3])
        .double_array_record(4, 1_100_000, &[1.1, 2.2, 3.3])
        .string_array_record(5, 1_100_000, &["a", "b", "c"])
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

    assert_eq!(rows.len(), 5);

    // Verify arrays are stored
    assert!(rows[0].data.get("/bool_array").unwrap().is_array());
    assert!(rows[1].data.get("/int_array").unwrap().is_array());
    assert!(rows[2].data.get("/float_array").unwrap().is_array());
    assert!(rows[3].data.get("/double_array").unwrap().is_array());
    assert!(rows[4].data.get("/string_array").unwrap().is_array());
}

#[test]
fn test_parse_with_finish_records() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("test.wpilog");

    let data = WpilogBuilder::new()
        .start_record(1_000_000, 1, "/sensor1", "double", "")
        .double_record(1, 1_100_000, 1.0)
        .double_record(1, 1_200_000, 2.0)
        .finish_record(1_300_000, 1)
        .start_record(1_400_000, 1, "/sensor2", "double", "") // Reuse ID
        .double_record(1, 1_500_000, 3.0)
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

    assert_eq!(rows.len(), 3);
    // First two records are from /sensor1
    assert!(rows[0].data.contains_key("/sensor1"));
    assert!(rows[1].data.contains_key("/sensor1"));
    // Last record is from /sensor2 (ID reused)
    assert!(rows[2].data.contains_key("/sensor2"));
}

#[test]
fn test_loop_count_increments() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("test.wpilog");

    let data = WpilogBuilder::new()
        .start_record(1_000_000, 1, "/Timestamp", "int64", "")
        .start_record(1_000_000, 2, "/sensor", "double", "")
        .int64_record(1, 1_000_000, 0) // Loop count should increment
        .double_record(2, 1_100_000, 1.0)
        .int64_record(1, 2_000_000, 1_000_000) // Loop count should increment again
        .double_record(2, 2_100_000, 2.0)
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

    // Reset loop count before test
    Formatter::reset_loop_count();

    formatter.read_wpilog(true).unwrap();
    let rows = formatter.read_wpilog(false).unwrap();

    assert_eq!(rows.len(), 4);
    assert_eq!(rows[0].loop_count, 0); // First /Timestamp increments to 1
    assert_eq!(rows[1].loop_count, 1);
    assert_eq!(rows[2].loop_count, 1); // Second /Timestamp increments to 2
    assert_eq!(rows[3].loop_count, 2);
}

#[test]
fn test_utf8_names_and_values() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("test.wpilog");

    let data = WpilogBuilder::new()
        .start_record(1_000_000, 1, "/传感器/温度", "string", "")
        .string_record(1, 1_100_000, "测试数据")
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

    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].data.get("/传感器/温度").unwrap().as_str().unwrap(),
        "测试数据"
    );
}

#[test]
fn test_large_dataset() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("test.wpilog");

    let mut builder = WpilogBuilder::new()
        .start_record(1_000_000, 1, "/sensor1", "double", "")
        .start_record(1_000_000, 2, "/sensor2", "double", "")
        .start_record(1_000_000, 3, "/sensor3", "double", "");

    // Add 1000 records
    for i in 0..1000 {
        let timestamp = 1_000_000 + i * 1000;
        builder = builder
            .double_record(1, timestamp, i as f64 * 0.1)
            .double_record(2, timestamp, i as f64 * 0.2)
            .double_record(3, timestamp, i as f64 * 0.3);
    }

    let data = builder.build();

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

    assert_eq!(rows.len(), 3000); // 1000 records * 3 sensors
}

#[test]
fn test_empty_wpilog_file() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("test.wpilog");

    let data = WpilogBuilder::new().build();

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

    assert_eq!(rows.len(), 0);
}

#[test]
fn test_metadata_tracking() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("test.wpilog");

    let data = WpilogBuilder::new()
        .start_record(
            1_000_000,
            1,
            "/sensor",
            "double",
            r#"{"source":"NT","unit":"m"}"#,
        )
        .double_record(1, 1_100_000, 1.5)
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

    assert_eq!(rows.len(), 1);
    // The metadata is stored in the StartRecordData but not directly in WideRow
    // We're mainly verifying it doesn't cause parsing errors
}

#[test]
fn test_json_type() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("test.wpilog");

    let data = WpilogBuilder::new()
        .start_record(1_000_000, 1, "/config", "json", "")
        .string_record(1, 1_100_000, r#"{"key":"value","number":42}"#)
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

    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].data.get("/config").unwrap().as_str().unwrap(),
        r#"{"key":"value","number":42}"#
    );
}

// ============================================================================
// STRUCT SCHEMA TESTS
// ============================================================================

#[test]
fn test_struct_schema_parsing() {
    use wpilog_parser::formatter::convert_struct_schema_to_columns;

    let schema_str = "double x; double y; double z";
    let columns = convert_struct_schema_to_columns(schema_str).unwrap();

    assert_eq!(columns.len(), 3);
    assert_eq!(columns[0].name, "x");
    assert_eq!(columns[0].type_name, "double");
    assert_eq!(columns[1].name, "y");
    assert_eq!(columns[1].type_name, "double");
    assert_eq!(columns[2].name, "z");
    assert_eq!(columns[2].type_name, "double");
}

#[test]
fn test_struct_schema_with_enum() {
    use wpilog_parser::formatter::convert_struct_schema_to_columns;

    let schema_str = "double x; enum {A, B, C} int32 mode";
    let columns = convert_struct_schema_to_columns(schema_str).unwrap();

    assert_eq!(columns.len(), 2);
    assert_eq!(columns[0].name, "x");
    assert_eq!(columns[0].type_name, "double");
    assert_eq!(columns[1].name, "mode");
    assert_eq!(columns[1].type_name, "int32");
}

#[test]
fn test_struct_schema_empty() {
    use wpilog_parser::formatter::convert_struct_schema_to_columns;

    let schema_str = "";
    let columns = convert_struct_schema_to_columns(schema_str).unwrap();

    assert_eq!(columns.len(), 0);
}

#[test]
fn test_column_name_sanitization() {
    use wpilog_parser::formatter::sanitize_column_name;

    // Currently sanitize_column_name just returns the string as-is
    assert_eq!(sanitize_column_name("test"), "test");
    assert_eq!(sanitize_column_name("/sensor/temp"), "/sensor/temp");
    assert_eq!(sanitize_column_name("with spaces"), "with spaces");
}

#[test]
fn test_struct_parsing_simple() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("test.wpilog");

    // Create a simple struct with double x, y, z
    let mut struct_data = Vec::new();
    struct_data.write_f64::<LittleEndian>(1.5).unwrap(); // x
    struct_data.write_f64::<LittleEndian>(2.5).unwrap(); // y
    struct_data.write_f64::<LittleEndian>(3.5).unwrap(); // z

    let data = WpilogBuilder::new()
        .struct_schema_record(1_000_000, 1, "struct:Point3D", "double x; double y; double z")
        .start_record(1_100_000, 2, "/robot/position", "struct:Point3D", "")
        .struct_record(2, 1_200_000, &struct_data)
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

    // First pass: infer schema
    Formatter::reset_loop_count();
    formatter.read_wpilog(true).unwrap();

    // Second pass: read data
    let rows = formatter.read_wpilog(false).unwrap();

    assert_eq!(rows.len(), 1);
    let row = &rows[0];

    // Verify struct data was parsed correctly
    let struct_value = row.data.get("/robot/position").unwrap();
    assert!(struct_value.is_object());

    let obj = struct_value.as_object().unwrap();
    assert_eq!(obj.get("x").unwrap().as_f64().unwrap(), 1.5);
    assert_eq!(obj.get("y").unwrap().as_f64().unwrap(), 2.5);
    assert_eq!(obj.get("z").unwrap().as_f64().unwrap(), 3.5);
}

#[test]
fn test_struct_parsing_mixed_types() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("test.wpilog");

    // Create a struct with supported types: int32 id, double value, float score
    let mut struct_data = Vec::new();
    struct_data.write_i32::<LittleEndian>(42).unwrap(); // id
    struct_data.write_f64::<LittleEndian>(99.9).unwrap(); // value
    struct_data.write_f32::<LittleEndian>(3.14).unwrap(); // score

    let data = WpilogBuilder::new()
        .struct_schema_record(1_000_000, 1, "struct:Sensor", "int32 id; double value; float score")
        .start_record(1_100_000, 2, "/sensor", "struct:Sensor", "")
        .struct_record(2, 1_200_000, &struct_data)
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

    Formatter::reset_loop_count();
    formatter.read_wpilog(true).unwrap();
    let rows = formatter.read_wpilog(false).unwrap();

    assert_eq!(rows.len(), 1);
    let struct_value = rows[0].data.get("/sensor").unwrap();
    let obj = struct_value.as_object().unwrap();

    assert_eq!(obj.get("id").unwrap().as_i64().unwrap(), 42);
    assert_eq!(obj.get("value").unwrap().as_f64().unwrap(), 99.9);
    assert!((obj.get("score").unwrap().as_f64().unwrap() - 3.14).abs() < 0.01);
}

#[test]
fn test_struct_with_int64() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("test.wpilog");

    // Create a struct with int64 and int32
    let mut struct_data = Vec::new();
    struct_data.write_i32::<LittleEndian>(42).unwrap(); // id
    struct_data.write_i64::<LittleEndian>(9000000000).unwrap(); // timestamp

    let data = WpilogBuilder::new()
        .struct_schema_record(1_000_000, 1, "struct:Data", "int32 id; int64 timestamp")
        .start_record(1_100_000, 2, "/data", "struct:Data", "")
        .struct_record(2, 1_200_000, &struct_data)
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

    Formatter::reset_loop_count();
    formatter.read_wpilog(true).unwrap();
    let rows = formatter.read_wpilog(false).unwrap();

    assert_eq!(rows.len(), 1);
    let struct_value = rows[0].data.get("/data").unwrap();
    let obj = struct_value.as_object().unwrap();

    assert_eq!(obj.get("id").unwrap().as_i64().unwrap(), 42);
    assert_eq!(obj.get("timestamp").unwrap().as_i64().unwrap(), 9000000000);
}
