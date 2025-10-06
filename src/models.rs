use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileFormat {
    Parquet,
    Avro,
    Json,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputFormat {
    Wide,
    Long,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DerivedSchemaColumn {
    pub name: String,
    #[serde(rename = "type")]
    pub type_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DerivedSchema {
    pub name: String,
    pub columns: Vec<DerivedSchemaColumn>,
}

#[derive(Debug, Clone, Serialize)]
pub struct WideRow {
    pub timestamp: f64,
    pub entry: u32,
    #[serde(rename = "type")]
    pub type_name: String,
    pub loop_count: u64,
    #[serde(flatten)]
    pub data: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize)]
pub struct NestedValue {
    pub double: Option<f64>,
    pub int64: Option<i64>,
    pub string: Option<String>,
    pub boolean: Option<bool>,
    pub boolean_array: Option<Vec<bool>>,
    pub double_array: Option<Vec<f64>>,
    pub float_array: Option<Vec<f32>>,
    pub int64_array: Option<Vec<i64>>,
    pub string_array: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct LongRow {
    pub timestamp: f64,
    pub entry: u32,
    #[serde(rename = "type")]
    pub type_name: String,
    pub json: Option<HashMap<String, serde_json::Value>>,
    pub value: Option<NestedValue>,
    pub loop_count: u64,
}

impl WideRow {
    pub fn new(timestamp: f64, entry: u32, type_name: String, loop_count: u64) -> Self {
        Self {
            timestamp,
            entry,
            type_name,
            loop_count,
            data: HashMap::new(),
        }
    }

    pub fn insert(&mut self, key: String, value: serde_json::Value) {
        self.data.insert(key, value);
    }
}

impl LongRow {
    pub fn new(timestamp: f64, entry: u32, type_name: String, loop_count: u64) -> Self {
        Self {
            timestamp,
            entry,
            type_name,
            json: None,
            value: Some(NestedValue {
                double: None,
                int64: None,
                string: None,
                boolean: None,
                boolean_array: None,
                double_array: None,
                float_array: None,
                int64_array: None,
                string_array: None,
            }),
            loop_count,
        }
    }
}
