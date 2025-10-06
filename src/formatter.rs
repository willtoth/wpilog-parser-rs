use anyhow::{anyhow, Result};
use byteorder::{LittleEndian, ReadBytesExt};
use memmap2::Mmap;
use serde_json::json;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::Cursor;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::datalog::{DataLogReader, DataLogRecord, StartRecordData};
use crate::models::{DerivedSchema, DerivedSchemaColumn, LongRow, OutputFormat, WideRow};

static LOOP_COUNT: AtomicU64 = AtomicU64::new(0);

pub fn sanitize_column_name(name: &str) -> String {
    name.to_string()
}

pub fn convert_struct_schema_to_columns(schema_str: &str) -> Result<Vec<DerivedSchemaColumn>> {
    let mut columns = Vec::new();

    for part in schema_str.split(';') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }

        // Handle enum inline
        if part.starts_with("enum") {
            if let Some(pos) = part.find('}') {
                let type_and_name = part[pos + 1..].trim();
                if let Some((typ, name)) = type_and_name.split_once(' ') {
                    columns.push(DerivedSchemaColumn {
                        name: name.to_string(),
                        type_name: typ.to_string(),
                    });
                }
            }
        } else if let Some((typ, name)) = part.split_once(' ') {
            columns.push(DerivedSchemaColumn {
                name: name.to_string(),
                type_name: typ.to_string(),
            });
        }
    }

    Ok(columns)
}

pub struct Formatter {
    pub wpilog_file: String,
    pub output_directory: String,
    pub output_format: OutputFormat,
    pub metrics_names: HashSet<String>,
    pub struct_schemas: Vec<DerivedSchema>,
}

impl Formatter {
    pub fn new(
        wpilog_file: String,
        output_directory: String,
        output_format: OutputFormat,
    ) -> Self {
        Self {
            wpilog_file,
            output_directory,
            output_format,
            metrics_names: HashSet::new(),
            struct_schemas: Vec::new(),
        }
    }

    pub fn parse_record_wide(
        &self,
        record: &DataLogRecord,
        entry: &StartRecordData,
    ) -> Result<WideRow> {
        let mut row = WideRow::new(
            record.timestamp as f64 / 1_000_000.0,
            record.entry,
            entry.type_name.clone(),
            LOOP_COUNT.load(Ordering::Relaxed),
        );

        if entry.name == "/Timestamp" {
            LOOP_COUNT.fetch_add(1, Ordering::Relaxed);
        }

        let sanitized_name = sanitize_column_name(&entry.name);

        match entry.type_name.as_str() {
            "double" => {
                row.insert(sanitized_name, json!(record.get_double()?));
            }
            "float" => {
                row.insert(sanitized_name, json!(record.get_float()?));
            }
            "int64" => {
                row.insert(sanitized_name, json!(record.get_integer()?));
            }
            "string" | "json" => {
                row.insert(sanitized_name, json!(record.get_string()?));
            }
            "boolean" => {
                row.insert(sanitized_name, json!(record.get_boolean()?));
            }
            "boolean[]" => {
                row.insert(sanitized_name, json!(record.get_boolean_array()));
            }
            "double[]" => {
                row.insert(sanitized_name, json!(record.get_double_array()?));
            }
            "float[]" => {
                row.insert(sanitized_name, json!(record.get_float_array()?));
            }
            "int64[]" => {
                row.insert(sanitized_name, json!(record.get_integer_array()?));
            }
            "string[]" => {
                row.insert(sanitized_name, json!(record.get_string_array()?));
            }
            "msgpack" => {
                row.insert(sanitized_name, json!(format!("{:?}", record.get_msgpack()?)));
            }
            "structschema" => {
                let _columns = convert_struct_schema_to_columns(&record.get_string()?)?;
                let _schema_name = entry
                    .name
                    .split(".schema/")
                    .nth(1)
                    .ok_or_else(|| anyhow!("Invalid schema name format"))?;

                // Store schema for later use
                // Note: we'd need to use interior mutability or restructure to modify self here
                row.insert(sanitized_name, json!(null));
            }
            type_name if type_name.starts_with("struct:") => {
                // Remove [] suffix if present to get schema name
                let schema_name = if type_name.ends_with("[]") {
                    &type_name[..type_name.len() - 2]
                } else {
                    type_name
                };

                let schema = self
                    .struct_schemas
                    .iter()
                    .find(|s| s.name == schema_name)
                    .ok_or_else(|| anyhow!("No struct schema found for: {}", schema_name))?;

                if record.data.is_empty() {
                    row.insert(entry.name.clone(), json!(null));
                } else {
                    let (struct_data, _bytes_consumed) = unpack_struct(&schema.columns, &record.data, 0, "", &self.struct_schemas)?;
                    row.insert(entry.name.clone(), json!(struct_data));
                }
            }
            type_name if type_name.contains("proto") => {
                row.insert(sanitized_name, json!(null)); // Proto data stored as bytes
            }
            _ => {
                row.insert(sanitized_name, json!(null));
            }
        }

        Ok(row)
    }

    pub fn parse_record_long(
        &self,
        record: &DataLogRecord,
        entry: &StartRecordData,
    ) -> Result<LongRow> {
        let mut row = LongRow::new(
            record.timestamp as f64 / 1_000_000.0,
            record.entry,
            entry.type_name.clone(),
            LOOP_COUNT.load(Ordering::Relaxed),
        );

        if entry.name == "/Timestamp" {
            LOOP_COUNT.fetch_add(1, Ordering::Relaxed);
        }

        if let Some(ref mut value) = row.value {
            match entry.type_name.as_str() {
                "double" => value.double = Some(record.get_double()?),
                "int64" => value.int64 = Some(record.get_integer()?),
                "string" => value.string = Some(record.get_string()?),
                "json" => {
                    let json_str = record.get_string()?;
                    row.json = Some(serde_json::from_str(&json_str)?);
                }
                "boolean" => value.boolean = Some(record.get_boolean()?),
                "boolean[]" => value.boolean_array = Some(record.get_boolean_array()),
                "double[]" => value.double_array = Some(record.get_double_array()?),
                "float[]" => value.float_array = Some(record.get_float_array()?),
                "int64[]" => value.int64_array = Some(record.get_integer_array()?),
                "string[]" => value.string_array = Some(record.get_string_array()?),
                _ => {}
            }
        }

        Ok(row)
    }

    pub fn read_wpilog(&mut self, infer_schema_only: bool) -> Result<Vec<WideRow>> {
        let file = File::open(&self.wpilog_file)?;
        let mmap = unsafe { Mmap::map(&file)? };
        self.read_wpilog_from_bytes(&mmap, infer_schema_only)
    }

    pub fn read_wpilog_from_bytes(&mut self, data: &[u8], infer_schema_only: bool) -> Result<Vec<WideRow>> {
        let mut records = Vec::new();
        let mut entries: HashMap<u32, StartRecordData> = HashMap::new();

        let reader = DataLogReader::new(data);

        if !reader.is_valid() {
            return Err(anyhow!("Not a valid WPILOG file"));
        }

        for record_result in reader.records()? {
            let record = record_result?;

            if record.is_start() {
                let data = record.get_start_data()?;
                entries.insert(data.entry, data);
            } else if record.is_finish() {
                let entry = record.get_finish_entry()?;
                entries.remove(&entry);
            } else if !record.is_control() {
                if let Some(entry) = entries.get(&record.entry) {
                    if infer_schema_only {
                        if entry.type_name == "structschema" {
                            let _columns = convert_struct_schema_to_columns(&record.get_string()?)?;
                            let _schema_name = entry
                                .name
                                .split(".schema/")
                                .nth(1)
                                .ok_or_else(|| anyhow!("Invalid schema name format"))?;

                            self.struct_schemas.push(DerivedSchema {
                                name: _schema_name.to_string(),
                                columns: _columns,
                            });
                        }
                    } else {
                        // Skip struct schema definition records in data pass
                        if entry.type_name != "structschema" {
                            let parsed_data = self.parse_record_wide(&record, entry)?;
                            self.metrics_names.insert(entry.name.clone());
                            records.push(parsed_data);
                        }
                    }
                }
            }
        }

        Ok(records)
    }

    pub fn reset_loop_count() {
        LOOP_COUNT.store(0, Ordering::Relaxed);
    }
}

/// Unpack a struct from binary data, matching Python implementation
///
/// Supports only: double, float, int32, int64, and nested structs
/// Does NOT support: arrays, strings, booleans, or other integer types within structs
fn unpack_struct(
    columns: &[DerivedSchemaColumn],
    data: &[u8],
    mut offset: usize,
    prefix: &str,
    schemas: &[DerivedSchema],
) -> Result<(HashMap<String, serde_json::Value>, usize)> {
    let mut result = HashMap::new();

    for col in columns {
        let key = if prefix.is_empty() {
            col.name.clone()
        } else {
            format!("{}.{}", prefix, col.name)
        };

        match col.type_name.as_str() {
            "double" => {
                if data.is_empty() {
                    result.insert(key, json!(null));
                } else {
                    if offset + 8 > data.len() {
                        return Err(anyhow!(
                            "Not enough data for double at offset {}, need 8 bytes but only {} available",
                            offset, data.len() - offset
                        ));
                    }
                    let mut cursor = Cursor::new(&data[offset..offset + 8]);
                    let val = cursor.read_f64::<LittleEndian>()?;
                    result.insert(key, json!(val));
                    offset += 8;
                }
            }
            "float" => {
                if data.is_empty() {
                    result.insert(key, json!(null));
                } else {
                    if offset + 4 > data.len() {
                        return Err(anyhow!("Not enough data for float at offset {}", offset));
                    }
                    let mut cursor = Cursor::new(&data[offset..offset + 4]);
                    let val = cursor.read_f32::<LittleEndian>()?;
                    result.insert(key, json!(val));
                    offset += 4;
                }
            }
            "int32" => {
                if data.is_empty() {
                    result.insert(key, json!(null));
                } else {
                    if offset + 4 > data.len() {
                        return Err(anyhow!("Not enough data for int32 at offset {}", offset));
                    }
                    let mut cursor = Cursor::new(&data[offset..offset + 4]);
                    let val = cursor.read_i32::<LittleEndian>()?;
                    result.insert(key, json!(val));
                    offset += 4;
                }
            }
            "int64" => {
                if data.is_empty() {
                    result.insert(key, json!(null));
                } else {
                    if offset + 8 > data.len() {
                        return Err(anyhow!("Not enough data for int64 at offset {}", offset));
                    }
                    let mut cursor = Cursor::new(&data[offset..offset + 8]);
                    let val = cursor.read_i64::<LittleEndian>()?;
                    result.insert(key, json!(val));
                    offset += 8;
                }
            }
            // Handle nested struct
            _ => {
                // Find nested schema - try with and without "struct:" prefix
                let nested_schema = schemas
                    .iter()
                    .find(|s| {
                        s.name.strip_prefix("struct:") == Some(&col.type_name) || s.name == col.type_name
                    })
                    .ok_or_else(|| anyhow!("No nested schema found for: {}", col.type_name))?;

                let (nested_result, new_offset) = unpack_struct(&nested_schema.columns, data, offset, &key, schemas)?;
                result.extend(nested_result);
                offset = new_offset;
            }
        };
    }

    Ok((result, offset))
}

