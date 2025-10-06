use anyhow::Result;
use arrow::array::{
    ArrayRef, BooleanArray, Float32Array, Float64Array, Int64Array, RecordBatch,
    StringArray, UInt32Array, ListBuilder, Float64Builder, Int64Builder, Float32Builder,
    BooleanBuilder, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema};
use log::info;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::collections::HashMap;
use std::fs::{create_dir_all, File};
use std::path::Path;
use std::sync::Arc;

use crate::models::WideRow;

pub struct ParquetFormatter {
    output_directory: String,
    chunk_size: usize,
}

impl ParquetFormatter {
    pub fn new(output_directory: String, chunk_size: usize) -> Self {
        Self {
            output_directory,
            chunk_size,
        }
    }

    pub fn convert(&self, rows: &[WideRow]) -> Result<()> {
        if rows.is_empty() {
            anyhow::bail!("No valid records to write to Parquet");
        }

        create_dir_all(&self.output_directory)?;

        let total_chunks = (rows.len() + self.chunk_size - 1) / self.chunk_size;
        info!(
            "Generated a total of {} chunks, will now create that total amount of files.",
            total_chunks
        );

        for (i, chunk) in rows.chunks(self.chunk_size).enumerate() {
            info!(
                "Writing chunk {}/{}, {} rows",
                i + 1,
                total_chunks,
                chunk.len()
            );

            let output_path = Path::new(&self.output_directory)
                .join(format!("file_part{:03}.parquet", i));

            self.write_chunk_to_parquet(chunk, &output_path)?;
        }

        info!("All chunks have been written");
        Ok(())
    }

    fn write_chunk_to_parquet(&self, rows: &[WideRow], output_path: &Path) -> Result<()> {
        // Build schema and infer types in a single pass
        let (all_columns, column_types) = self.infer_schema_single_pass(rows);

        let mut fields = vec![
            Field::new("timestamp", DataType::Float64, false),
            Field::new("entry", DataType::UInt32, false),
            Field::new("type", DataType::Utf8, false),
            Field::new("loop_count", DataType::Int64, false),
        ];

        // Add dynamic fields with inferred types (already sorted)
        for col_name in &all_columns {
            let data_type = column_types.get(col_name).cloned().unwrap_or(DataType::Utf8);
            fields.push(Field::new(col_name.as_str(), data_type, true));
        }

        let schema = Arc::new(Schema::new(fields));

        // Build arrays with pre-allocated capacity
        let num_rows = rows.len();
        let mut timestamp_vec = Vec::with_capacity(num_rows);
        let mut entry_vec = Vec::with_capacity(num_rows);
        let mut type_vec = Vec::with_capacity(num_rows);
        let mut loop_count_vec = Vec::with_capacity(num_rows);

        for row in rows {
            timestamp_vec.push(row.timestamp);
            entry_vec.push(row.entry);
            type_vec.push(row.type_name.as_str());
            loop_count_vec.push(row.loop_count as i64);
        }

        let timestamps: ArrayRef = Arc::new(Float64Array::from(timestamp_vec));
        let entries: ArrayRef = Arc::new(UInt32Array::from(entry_vec));
        let types: ArrayRef = Arc::new(StringArray::from(type_vec));
        let loop_counts: ArrayRef = Arc::new(Int64Array::from(loop_count_vec));

        let mut arrays: Vec<ArrayRef> = vec![timestamps, entries, types, loop_counts];

        // Add dynamic columns with proper types
        for col_name in &all_columns {
            let data_type = column_types.get(col_name).cloned().unwrap_or(DataType::Utf8);
            let array = self.build_typed_array(rows, col_name, &data_type)?;
            arrays.push(array);
        }

        let batch = RecordBatch::try_new(schema.clone(), arrays)?;

        let file = File::create(output_path)?;
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;

        writer.write(&batch)?;
        writer.close()?;

        Ok(())
    }

    fn infer_schema_single_pass(&self, rows: &[WideRow]) -> (Vec<String>, HashMap<String, DataType>) {
        let mut column_types = HashMap::new();
        let mut column_order = Vec::new();

        for row in rows {
            for (col_name, value) in &row.data {
                // Only process if we haven't seen this column yet
                if column_types.contains_key(col_name) {
                    continue;
                }

                if !value.is_null() {
                    let data_type = match value {
                        serde_json::Value::Bool(_) => DataType::Boolean,
                        serde_json::Value::Number(n) => {
                            if n.is_f64() {
                                DataType::Float64
                            } else if n.is_i64() {
                                DataType::Int64
                            } else {
                                DataType::Float64
                            }
                        }
                        serde_json::Value::String(_) => DataType::Utf8,
                        serde_json::Value::Array(arr) => {
                            if let Some(first) = arr.first() {
                                match first {
                                    serde_json::Value::Bool(_) => {
                                        DataType::List(Arc::new(Field::new("item", DataType::Boolean, true)))
                                    }
                                    serde_json::Value::Number(n) => {
                                        if n.is_f64() {
                                            DataType::List(Arc::new(Field::new("item", DataType::Float64, true)))
                                        } else if n.is_i64() {
                                            DataType::List(Arc::new(Field::new("item", DataType::Int64, true)))
                                        } else {
                                            DataType::List(Arc::new(Field::new("item", DataType::Float64, true)))
                                        }
                                    }
                                    serde_json::Value::String(_) => {
                                        DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))
                                    }
                                    _ => DataType::Utf8, // Complex nested types as JSON
                                }
                            } else {
                                // Empty array - default to string list
                                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))
                            }
                        }
                        serde_json::Value::Object(_) => DataType::Utf8, // Store JSON objects as strings
                        serde_json::Value::Null => continue, // Skip nulls
                    };
                    column_types.insert(col_name.clone(), data_type);
                    column_order.push(col_name.clone());
                }
            }
        }

        // Sort column names for consistent output
        column_order.sort();

        (column_order, column_types)
    }

    fn build_typed_array(&self, rows: &[WideRow], col_name: &str, data_type: &DataType) -> Result<ArrayRef> {
        match data_type {
            DataType::Boolean => {
                let values: Vec<Option<bool>> = rows
                    .iter()
                    .map(|r| {
                        r.data.get(col_name).and_then(|v| v.as_bool())
                    })
                    .collect();
                Ok(Arc::new(BooleanArray::from(values)))
            }
            DataType::Int64 => {
                let values: Vec<Option<i64>> = rows
                    .iter()
                    .map(|r| {
                        r.data.get(col_name).and_then(|v| v.as_i64())
                    })
                    .collect();
                Ok(Arc::new(Int64Array::from(values)))
            }
            DataType::Float64 => {
                let values: Vec<Option<f64>> = rows
                    .iter()
                    .map(|r| {
                        r.data.get(col_name).and_then(|v| v.as_f64())
                    })
                    .collect();
                Ok(Arc::new(Float64Array::from(values)))
            }
            DataType::Float32 => {
                let values: Vec<Option<f32>> = rows
                    .iter()
                    .map(|r| {
                        r.data.get(col_name).and_then(|v| v.as_f64().map(|f| f as f32))
                    })
                    .collect();
                Ok(Arc::new(Float32Array::from(values)))
            }
            DataType::List(field) => {
                // Build ListArray based on element type
                match field.data_type() {
                    DataType::Boolean => {
                        let mut builder = ListBuilder::new(BooleanBuilder::new());
                        for row in rows {
                            if let Some(value) = row.data.get(col_name) {
                                if let Some(arr) = value.as_array() {
                                    for elem in arr {
                                        builder.values().append_option(elem.as_bool());
                                    }
                                    builder.append(true);
                                } else {
                                    builder.append(false);
                                }
                            } else {
                                builder.append(false);
                            }
                        }
                        Ok(Arc::new(builder.finish()))
                    }
                    DataType::Int64 => {
                        let mut builder = ListBuilder::new(Int64Builder::new());
                        for row in rows {
                            if let Some(value) = row.data.get(col_name) {
                                if let Some(arr) = value.as_array() {
                                    for elem in arr {
                                        builder.values().append_option(elem.as_i64());
                                    }
                                    builder.append(true);
                                } else {
                                    builder.append(false);
                                }
                            } else {
                                builder.append(false);
                            }
                        }
                        Ok(Arc::new(builder.finish()))
                    }
                    DataType::Float64 => {
                        let mut builder = ListBuilder::new(Float64Builder::new());
                        for row in rows {
                            if let Some(value) = row.data.get(col_name) {
                                if let Some(arr) = value.as_array() {
                                    for elem in arr {
                                        builder.values().append_option(elem.as_f64());
                                    }
                                    builder.append(true);
                                } else {
                                    builder.append(false);
                                }
                            } else {
                                builder.append(false);
                            }
                        }
                        Ok(Arc::new(builder.finish()))
                    }
                    DataType::Float32 => {
                        let mut builder = ListBuilder::new(Float32Builder::new());
                        for row in rows {
                            if let Some(value) = row.data.get(col_name) {
                                if let Some(arr) = value.as_array() {
                                    for elem in arr {
                                        builder.values().append_option(elem.as_f64().map(|f| f as f32));
                                    }
                                    builder.append(true);
                                } else {
                                    builder.append(false);
                                }
                            } else {
                                builder.append(false);
                            }
                        }
                        Ok(Arc::new(builder.finish()))
                    }
                    DataType::Utf8 => {
                        let mut builder = ListBuilder::new(StringBuilder::new());
                        for row in rows {
                            if let Some(value) = row.data.get(col_name) {
                                if let Some(arr) = value.as_array() {
                                    for elem in arr {
                                        builder.values().append_option(elem.as_str());
                                    }
                                    builder.append(true);
                                } else {
                                    builder.append(false);
                                }
                            } else {
                                builder.append(false);
                            }
                        }
                        Ok(Arc::new(builder.finish()))
                    }
                    _ => {
                        // Unsupported list element type, fallback to JSON string
                        let values: Vec<Option<String>> = rows
                            .iter()
                            .map(|r| {
                                r.data.get(col_name).map(|v| serde_json::to_string(v).unwrap_or_default())
                            })
                            .collect();
                        Ok(Arc::new(StringArray::from(values)))
                    }
                }
            }
            DataType::Utf8 | _ => {
                let values: Vec<Option<String>> = rows
                    .iter()
                    .map(|r| {
                        r.data.get(col_name).map(|v| match v {
                            serde_json::Value::Null => "null".to_string(),
                            serde_json::Value::Bool(b) => b.to_string(),
                            serde_json::Value::Number(n) => n.to_string(),
                            serde_json::Value::String(s) => s.clone(),
                            serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
                                serde_json::to_string(v).unwrap_or_default()
                            }
                        })
                    })
                    .collect();
                Ok(Arc::new(StringArray::from(values)))
            }
        }
    }
}
