mod common;

use common::WpilogBuilder;
use wpilog_parser::datalog::DataLogReader;

// ============================================================================
// HEADER TESTS
// ============================================================================

#[test]
fn test_valid_header_minimal() {
    let data = WpilogBuilder::new().build();
    let reader = DataLogReader::new(&data);
    assert!(reader.is_valid());
    assert_eq!(reader.get_version(), 0x0100);
    assert_eq!(reader.get_extra_header(), "");
}

#[test]
fn test_valid_header_with_extra_header() {
    let data = WpilogBuilder::with_header(0x0100, "test extra header").build();
    let reader = DataLogReader::new(&data);
    assert!(reader.is_valid());
    assert_eq!(reader.get_version(), 0x0100);
    assert_eq!(reader.get_extra_header(), "test extra header");
}

#[test]
fn test_invalid_magic_bytes() {
    let mut data = WpilogBuilder::new().build();
    data[0] = b'X'; // Corrupt magic bytes
    let reader = DataLogReader::new(&data);
    assert!(!reader.is_valid());
}

#[test]
fn test_invalid_version_too_old() {
    let data = WpilogBuilder::with_header(0x0099, "").build();
    let reader = DataLogReader::new(&data);
    assert!(!reader.is_valid());
}

#[test]
fn test_file_too_short() {
    let data = vec![0x57, 0x50, 0x49, 0x4c]; // Only "WPIL"
    let reader = DataLogReader::new(&data);
    assert!(!reader.is_valid());
}

#[test]
fn test_empty_file() {
    let data = vec![];
    let reader = DataLogReader::new(&data);
    assert!(!reader.is_valid());
}

#[test]
fn test_version_parsing() {
    let data = WpilogBuilder::with_header(0x0205, "").build();
    let reader = DataLogReader::new(&data);
    assert!(reader.is_valid());
    assert_eq!(reader.get_version(), 0x0205);
}

#[test]
fn test_extra_header_utf8() {
    let data = WpilogBuilder::with_header(0x0100, "Hello 世界 🌍").build();
    let reader = DataLogReader::new(&data);
    assert!(reader.is_valid());
    assert_eq!(reader.get_extra_header(), "Hello 世界 🌍");
}

// ============================================================================
// CONTROL RECORD TESTS
// ============================================================================

#[test]
fn test_start_record_basic() {
    let data = WpilogBuilder::new()
        .start_record(1_000_000, 1, "test", "int64", "")
        .build();

    let reader = DataLogReader::new(&data);
    let records: Vec<_> = reader.records().unwrap().collect();

    assert_eq!(records.len(), 1);
    let record = records[0].as_ref().unwrap();
    assert!(record.is_control());
    assert!(record.is_start());
    assert!(!record.is_finish());
    assert!(!record.is_set_metadata());

    let start_data = record.get_start_data().unwrap();
    assert_eq!(start_data.entry, 1);
    assert_eq!(start_data.name, "test");
    assert_eq!(start_data.type_name, "int64");
    assert_eq!(start_data.metadata, "");
}

#[test]
fn test_start_record_with_metadata() {
    let data = WpilogBuilder::new()
        .start_record(
            1_000_000,
            1,
            "sensor",
            "double",
            r#"{"source":"NT","unit":"meters"}"#,
        )
        .build();

    let reader = DataLogReader::new(&data);
    let records: Vec<_> = reader.records().unwrap().collect();

    assert_eq!(records.len(), 1);
    let record = records[0].as_ref().unwrap();
    let start_data = record.get_start_data().unwrap();
    assert_eq!(start_data.entry, 1);
    assert_eq!(start_data.name, "sensor");
    assert_eq!(start_data.type_name, "double");
    assert_eq!(start_data.metadata, r#"{"source":"NT","unit":"meters"}"#);
}

#[test]
fn test_start_record_utf8_names() {
    let data = WpilogBuilder::new()
        .start_record(1_000_000, 1, "/传感器/温度", "double", "温度数据")
        .build();

    let reader = DataLogReader::new(&data);
    let records: Vec<_> = reader.records().unwrap().collect();

    let record = records[0].as_ref().unwrap();
    let start_data = record.get_start_data().unwrap();
    assert_eq!(start_data.name, "/传感器/温度");
    assert_eq!(start_data.metadata, "温度数据");
}

#[test]
fn test_finish_record() {
    let data = WpilogBuilder::new().finish_record(2_000_000, 1).build();

    let reader = DataLogReader::new(&data);
    let records: Vec<_> = reader.records().unwrap().collect();

    assert_eq!(records.len(), 1);
    let record = records[0].as_ref().unwrap();
    assert!(record.is_control());
    assert!(record.is_finish());
    assert!(!record.is_start());
    assert_eq!(record.get_finish_entry().unwrap(), 1);
}

#[test]
fn test_set_metadata_record() {
    let data = WpilogBuilder::new()
        .set_metadata_record(1_500_000, 1, r#"{"updated":"true"}"#)
        .build();

    let reader = DataLogReader::new(&data);
    let records: Vec<_> = reader.records().unwrap().collect();

    assert_eq!(records.len(), 1);
    let record = records[0].as_ref().unwrap();
    assert!(record.is_control());
    assert!(record.is_set_metadata());
    assert!(!record.is_start());
    assert!(!record.is_finish());

    let metadata_data = record.get_set_metadata_data().unwrap();
    assert_eq!(metadata_data.entry, 1);
    assert_eq!(metadata_data.metadata, r#"{"updated":"true"}"#);
}

#[test]
fn test_complete_entry_lifecycle() {
    let data = WpilogBuilder::new()
        .start_record(1_000_000, 1, "test", "int64", "")
        .int64_record(1, 1_100_000, 42)
        .int64_record(1, 1_200_000, 43)
        .finish_record(1_300_000, 1)
        .build();

    let reader = DataLogReader::new(&data);
    let records: Vec<_> = reader.records().unwrap().collect();

    assert_eq!(records.len(), 4);
    assert!(records[0].as_ref().unwrap().is_start());
    assert_eq!(records[1].as_ref().unwrap().entry, 1);
    assert_eq!(records[2].as_ref().unwrap().entry, 1);
    assert!(records[3].as_ref().unwrap().is_finish());
}

// ============================================================================
// DATA TYPE TESTS
// ============================================================================

#[test]
fn test_boolean_true() {
    let data = WpilogBuilder::new()
        .start_record(1_000_000, 1, "test", "boolean", "")
        .boolean_record(1, 1_100_000, true)
        .build();

    let reader = DataLogReader::new(&data);
    let records: Vec<_> = reader.records().unwrap().collect();

    let record = &records[1].as_ref().unwrap();
    assert_eq!(record.get_boolean().unwrap(), true);
}

#[test]
fn test_boolean_false() {
    let data = WpilogBuilder::new()
        .start_record(1_000_000, 1, "test", "boolean", "")
        .boolean_record(1, 1_100_000, false)
        .build();

    let reader = DataLogReader::new(&data);
    let records: Vec<_> = reader.records().unwrap().collect();

    let record = &records[1].as_ref().unwrap();
    assert_eq!(record.get_boolean().unwrap(), false);
}

#[test]
fn test_int64_positive() {
    let data = WpilogBuilder::new()
        .start_record(1_000_000, 1, "test", "int64", "")
        .int64_record(1, 1_100_000, 42)
        .build();

    let reader = DataLogReader::new(&data);
    let records: Vec<_> = reader.records().unwrap().collect();

    let record = &records[1].as_ref().unwrap();
    assert_eq!(record.get_integer().unwrap(), 42);
}

#[test]
fn test_int64_negative() {
    let data = WpilogBuilder::new()
        .start_record(1_000_000, 1, "test", "int64", "")
        .int64_record(1, 1_100_000, -42)
        .build();

    let reader = DataLogReader::new(&data);
    let records: Vec<_> = reader.records().unwrap().collect();

    let record = &records[1].as_ref().unwrap();
    assert_eq!(record.get_integer().unwrap(), -42);
}

#[test]
fn test_int64_max() {
    let data = WpilogBuilder::new()
        .start_record(1_000_000, 1, "test", "int64", "")
        .int64_record(1, 1_100_000, i64::MAX)
        .build();

    let reader = DataLogReader::new(&data);
    let records: Vec<_> = reader.records().unwrap().collect();

    let record = &records[1].as_ref().unwrap();
    assert_eq!(record.get_integer().unwrap(), i64::MAX);
}

#[test]
fn test_int64_min() {
    let data = WpilogBuilder::new()
        .start_record(1_000_000, 1, "test", "int64", "")
        .int64_record(1, 1_100_000, i64::MIN)
        .build();

    let reader = DataLogReader::new(&data);
    let records: Vec<_> = reader.records().unwrap().collect();

    let record = &records[1].as_ref().unwrap();
    assert_eq!(record.get_integer().unwrap(), i64::MIN);
}

#[test]
fn test_float() {
    let data = WpilogBuilder::new()
        .start_record(1_000_000, 1, "test", "float", "")
        .float_record(1, 1_100_000, 3.14159)
        .build();

    let reader = DataLogReader::new(&data);
    let records: Vec<_> = reader.records().unwrap().collect();

    let record = &records[1].as_ref().unwrap();
    let value = record.get_float().unwrap();
    assert!((value - 3.14159).abs() < 0.0001);
}

#[test]
fn test_double() {
    let data = WpilogBuilder::new()
        .start_record(1_000_000, 1, "test", "double", "")
        .double_record(1, 1_100_000, 3.141592653589793)
        .build();

    let reader = DataLogReader::new(&data);
    let records: Vec<_> = reader.records().unwrap().collect();

    let record = &records[1].as_ref().unwrap();
    let value = record.get_double().unwrap();
    assert!((value - 3.141592653589793).abs() < 1e-10);
}

#[test]
fn test_string() {
    let data = WpilogBuilder::new()
        .start_record(1_000_000, 1, "test", "string", "")
        .string_record(1, 1_100_000, "Hello, World!")
        .build();

    let reader = DataLogReader::new(&data);
    let records: Vec<_> = reader.records().unwrap().collect();

    let record = &records[1].as_ref().unwrap();
    assert_eq!(record.get_string().unwrap(), "Hello, World!");
}

#[test]
fn test_string_empty() {
    let data = WpilogBuilder::new()
        .start_record(1_000_000, 1, "test", "string", "")
        .string_record(1, 1_100_000, "")
        .build();

    let reader = DataLogReader::new(&data);
    let records: Vec<_> = reader.records().unwrap().collect();

    let record = &records[1].as_ref().unwrap();
    assert_eq!(record.get_string().unwrap(), "");
}

#[test]
fn test_string_utf8() {
    let data = WpilogBuilder::new()
        .start_record(1_000_000, 1, "test", "string", "")
        .string_record(1, 1_100_000, "Hello 世界 🌍")
        .build();

    let reader = DataLogReader::new(&data);
    let records: Vec<_> = reader.records().unwrap().collect();

    let record = &records[1].as_ref().unwrap();
    assert_eq!(record.get_string().unwrap(), "Hello 世界 🌍");
}

#[test]
fn test_boolean_array() {
    let data = WpilogBuilder::new()
        .start_record(1_000_000, 1, "test", "boolean[]", "")
        .boolean_array_record(1, 1_100_000, &[true, false, true, true, false])
        .build();

    let reader = DataLogReader::new(&data);
    let records: Vec<_> = reader.records().unwrap().collect();

    let record = &records[1].as_ref().unwrap();
    let values = record.get_boolean_array();
    assert_eq!(values, vec![true, false, true, true, false]);
}

#[test]
fn test_boolean_array_empty() {
    let data = WpilogBuilder::new()
        .start_record(1_000_000, 1, "test", "boolean[]", "")
        .boolean_array_record(1, 1_100_000, &[])
        .build();

    let reader = DataLogReader::new(&data);
    let records: Vec<_> = reader.records().unwrap().collect();

    let record = &records[1].as_ref().unwrap();
    let values = record.get_boolean_array();
    assert_eq!(values, Vec::<bool>::new());
}

#[test]
fn test_int64_array() {
    let data = WpilogBuilder::new()
        .start_record(1_000_000, 1, "test", "int64[]", "")
        .int64_array_record(1, 1_100_000, &[1, 2, 3, -4, 5])
        .build();

    let reader = DataLogReader::new(&data);
    let records: Vec<_> = reader.records().unwrap().collect();

    let record = &records[1].as_ref().unwrap();
    let values = record.get_integer_array().unwrap();
    assert_eq!(values, vec![1, 2, 3, -4, 5]);
}

#[test]
fn test_int64_array_empty() {
    let data = WpilogBuilder::new()
        .start_record(1_000_000, 1, "test", "int64[]", "")
        .int64_array_record(1, 1_100_000, &[])
        .build();

    let reader = DataLogReader::new(&data);
    let records: Vec<_> = reader.records().unwrap().collect();

    let record = &records[1].as_ref().unwrap();
    let values = record.get_integer_array().unwrap();
    assert_eq!(values, Vec::<i64>::new());
}

#[test]
fn test_float_array() {
    let data = WpilogBuilder::new()
        .start_record(1_000_000, 1, "test", "float[]", "")
        .float_array_record(1, 1_100_000, &[1.1, 2.2, 3.3])
        .build();

    let reader = DataLogReader::new(&data);
    let records: Vec<_> = reader.records().unwrap().collect();

    let record = &records[1].as_ref().unwrap();
    let values = record.get_float_array().unwrap();
    assert_eq!(values.len(), 3);
    assert!((values[0] - 1.1).abs() < 0.001);
    assert!((values[1] - 2.2).abs() < 0.001);
    assert!((values[2] - 3.3).abs() < 0.001);
}

#[test]
fn test_double_array() {
    let data = WpilogBuilder::new()
        .start_record(1_000_000, 1, "test", "double[]", "")
        .double_array_record(1, 1_100_000, &[1.1, 2.2, 3.3])
        .build();

    let reader = DataLogReader::new(&data);
    let records: Vec<_> = reader.records().unwrap().collect();

    let record = &records[1].as_ref().unwrap();
    let values = record.get_double_array().unwrap();
    assert_eq!(values.len(), 3);
    assert!((values[0] - 1.1).abs() < 1e-10);
    assert!((values[1] - 2.2).abs() < 1e-10);
    assert!((values[2] - 3.3).abs() < 1e-10);
}

#[test]
fn test_string_array() {
    let data = WpilogBuilder::new()
        .start_record(1_000_000, 1, "test", "string[]", "")
        .string_array_record(1, 1_100_000, &["hello", "world", "test"])
        .build();

    let reader = DataLogReader::new(&data);
    let records: Vec<_> = reader.records().unwrap().collect();

    let record = &records[1].as_ref().unwrap();
    let values = record.get_string_array().unwrap();
    assert_eq!(values, vec!["hello", "world", "test"]);
}

#[test]
fn test_string_array_empty() {
    let data = WpilogBuilder::new()
        .start_record(1_000_000, 1, "test", "string[]", "")
        .string_array_record(1, 1_100_000, &[])
        .build();

    let reader = DataLogReader::new(&data);
    let records: Vec<_> = reader.records().unwrap().collect();

    let record = &records[1].as_ref().unwrap();
    let values = record.get_string_array().unwrap();
    assert_eq!(values, Vec::<String>::new());
}

#[test]
fn test_string_array_with_utf8() {
    let data = WpilogBuilder::new()
        .start_record(1_000_000, 1, "test", "string[]", "")
        .string_array_record(1, 1_100_000, &["hello", "世界", "🌍"])
        .build();

    let reader = DataLogReader::new(&data);
    let records: Vec<_> = reader.records().unwrap().collect();

    let record = &records[1].as_ref().unwrap();
    let values = record.get_string_array().unwrap();
    assert_eq!(values, vec!["hello", "世界", "🌍"]);
}

// ============================================================================
// VARIABLE-LENGTH ENCODING TESTS
// ============================================================================

#[test]
fn test_variable_length_entry_id_1_byte() {
    let data = WpilogBuilder::new()
        .start_record(1_000_000, 1, "test", "int64", "")
        .int64_record(1, 1_100_000, 42)
        .build();

    let reader = DataLogReader::new(&data);
    let records: Vec<_> = reader.records().unwrap().collect();
    assert_eq!(records.len(), 2);
}

#[test]
fn test_variable_length_entry_id_2_bytes() {
    let data = WpilogBuilder::new()
        .start_record(1_000_000, 256, "test", "int64", "")
        .int64_record(256, 1_100_000, 42)
        .build();

    let reader = DataLogReader::new(&data);
    let records: Vec<_> = reader.records().unwrap().collect();

    assert_eq!(records.len(), 2);
    let start = records[0].as_ref().unwrap();
    assert_eq!(start.get_start_data().unwrap().entry, 256);
    let data_record = records[1].as_ref().unwrap();
    assert_eq!(data_record.entry, 256);
}

#[test]
fn test_variable_length_entry_id_3_bytes() {
    let data = WpilogBuilder::new()
        .start_record(1_000_000, 0x10000, "test", "int64", "")
        .int64_record(0x10000, 1_100_000, 42)
        .build();

    let reader = DataLogReader::new(&data);
    let records: Vec<_> = reader.records().unwrap().collect();

    assert_eq!(records.len(), 2);
    let data_record = records[1].as_ref().unwrap();
    assert_eq!(data_record.entry, 0x10000);
}

#[test]
fn test_variable_length_timestamp_1_byte() {
    let data = WpilogBuilder::new()
        .start_record(1, 1, "test", "int64", "")
        .int64_record(1, 2, 42)
        .build();

    let reader = DataLogReader::new(&data);
    let records: Vec<_> = reader.records().unwrap().collect();

    let record = &records[1].as_ref().unwrap();
    assert_eq!(record.timestamp, 2);
}

#[test]
fn test_variable_length_timestamp_8_bytes() {
    let timestamp = u64::MAX;
    let data = WpilogBuilder::new()
        .start_record(timestamp, 1, "test", "int64", "")
        .int64_record(1, timestamp, 42)
        .build();

    let reader = DataLogReader::new(&data);
    let records: Vec<_> = reader.records().unwrap().collect();

    let record = &records[1].as_ref().unwrap();
    assert_eq!(record.timestamp, timestamp);
}

// ============================================================================
// EDGE CASE TESTS
// ============================================================================

#[test]
fn test_out_of_order_timestamps() {
    let data = WpilogBuilder::new()
        .start_record(3_000_000, 1, "test", "int64", "")
        .int64_record(1, 1_000_000, 1) // Earlier timestamp
        .int64_record(1, 3_000_000, 2)
        .int64_record(1, 2_000_000, 3) // Out of order
        .build();

    let reader = DataLogReader::new(&data);
    let records: Vec<_> = reader.records().unwrap().collect();

    assert_eq!(records.len(), 4);
    assert_eq!(records[1].as_ref().unwrap().timestamp, 1_000_000);
    assert_eq!(records[2].as_ref().unwrap().timestamp, 3_000_000);
    assert_eq!(records[3].as_ref().unwrap().timestamp, 2_000_000);
}

#[test]
fn test_entry_reuse_after_finish() {
    let data = WpilogBuilder::new()
        .start_record(1_000_000, 1, "test1", "int64", "")
        .int64_record(1, 1_100_000, 42)
        .finish_record(1_200_000, 1)
        .start_record(1_300_000, 1, "test2", "double", "") // Reuse entry ID 1
        .double_record(1, 1_400_000, 3.14)
        .build();

    let reader = DataLogReader::new(&data);
    let records: Vec<_> = reader.records().unwrap().collect();

    assert_eq!(records.len(), 5);
    let first_start = records[0].as_ref().unwrap().get_start_data().unwrap();
    assert_eq!(first_start.name, "test1");
    assert_eq!(first_start.type_name, "int64");

    let second_start = records[3].as_ref().unwrap().get_start_data().unwrap();
    assert_eq!(second_start.name, "test2");
    assert_eq!(second_start.type_name, "double");
}

#[test]
fn test_multiple_entries() {
    let data = WpilogBuilder::new()
        .start_record(1_000_000, 1, "test1", "int64", "")
        .start_record(1_000_000, 2, "test2", "double", "")
        .start_record(1_000_000, 3, "test3", "string", "")
        .int64_record(1, 1_100_000, 42)
        .double_record(2, 1_100_000, 3.14)
        .string_record(3, 1_100_000, "hello")
        .build();

    let reader = DataLogReader::new(&data);
    let records: Vec<_> = reader.records().unwrap().collect();

    assert_eq!(records.len(), 6);
    assert_eq!(records[3].as_ref().unwrap().get_integer().unwrap(), 42);
    assert!((records[4].as_ref().unwrap().get_double().unwrap() - 3.14).abs() < 0.001);
    assert_eq!(records[5].as_ref().unwrap().get_string().unwrap(), "hello");
}

#[test]
fn test_metadata_update() {
    let data = WpilogBuilder::new()
        .start_record(1_000_000, 1, "test", "int64", r#"{"version":1}"#)
        .int64_record(1, 1_100_000, 42)
        .set_metadata_record(1_200_000, 1, r#"{"version":2}"#)
        .int64_record(1, 1_300_000, 43)
        .build();

    let reader = DataLogReader::new(&data);
    let records: Vec<_> = reader.records().unwrap().collect();

    assert_eq!(records.len(), 4);
    let start = records[0].as_ref().unwrap();
    assert_eq!(start.get_start_data().unwrap().metadata, r#"{"version":1}"#);

    let update = records[2].as_ref().unwrap();
    assert_eq!(
        update.get_set_metadata_data().unwrap().metadata,
        r#"{"version":2}"#
    );
}

#[test]
fn test_large_payload() {
    let large_string = "x".repeat(10000);
    let data = WpilogBuilder::new()
        .start_record(1_000_000, 1, "test", "string", "")
        .string_record(1, 1_100_000, &large_string)
        .build();

    let reader = DataLogReader::new(&data);
    let records: Vec<_> = reader.records().unwrap().collect();

    let record = &records[1].as_ref().unwrap();
    assert_eq!(record.get_string().unwrap(), large_string);
}

#[test]
fn test_many_records() {
    let mut builder = WpilogBuilder::new().start_record(1_000_000, 1, "test", "int64", "");

    for i in 0..1000 {
        builder = builder.int64_record(1, 1_000_000 + i * 1000, i as i64);
    }

    let data = builder.build();
    let reader = DataLogReader::new(&data);
    let records: Vec<_> = reader.records().unwrap().collect();

    assert_eq!(records.len(), 1001); // 1 start + 1000 data
}

#[test]
fn test_zero_timestamp() {
    let data = WpilogBuilder::new()
        .start_record(0, 1, "test", "int64", "")
        .int64_record(1, 0, 42)
        .build();

    let reader = DataLogReader::new(&data);
    let records: Vec<_> = reader.records().unwrap().collect();

    assert_eq!(records[0].as_ref().unwrap().timestamp, 0);
    assert_eq!(records[1].as_ref().unwrap().timestamp, 0);
}
