/// Test utilities for building WPILOG files
use byteorder::{LittleEndian, WriteBytesExt};

/// Builder for creating WPILOG test files
pub struct WpilogBuilder {
    data: Vec<u8>,
}

impl WpilogBuilder {
    /// Create a new WPILOG builder with default header (version 1.0, no extra header)
    pub fn new() -> Self {
        let mut builder = Self { data: Vec::new() };
        builder.write_header(0x0100, "");
        builder
    }

    /// Create a new builder with a specific version and extra header
    pub fn with_header(version: u16, extra_header: &str) -> Self {
        let mut builder = Self { data: Vec::new() };
        builder.write_header(version, extra_header);
        builder
    }

    /// Write the WPILOG header
    fn write_header(&mut self, version: u16, extra_header: &str) {
        self.data.extend_from_slice(b"WPILOG");
        self.data.write_u16::<LittleEndian>(version).unwrap();
        self.data
            .write_u32::<LittleEndian>(extra_header.len() as u32)
            .unwrap();
        self.data.extend_from_slice(extra_header.as_bytes());
    }

    /// Add a Start control record
    pub fn start_record(
        mut self,
        timestamp: u64,
        entry_id: u32,
        name: &str,
        type_str: &str,
        metadata: &str,
    ) -> Self {
        let mut payload = Vec::new();
        payload.push(0); // Start control type
        payload.write_u32::<LittleEndian>(entry_id).unwrap();
        payload.write_u32::<LittleEndian>(name.len() as u32).unwrap();
        payload.extend_from_slice(name.as_bytes());
        payload
            .write_u32::<LittleEndian>(type_str.len() as u32)
            .unwrap();
        payload.extend_from_slice(type_str.as_bytes());
        payload
            .write_u32::<LittleEndian>(metadata.len() as u32)
            .unwrap();
        payload.extend_from_slice(metadata.as_bytes());

        self.write_record(0, timestamp, &payload);
        self
    }

    /// Add a Finish control record
    pub fn finish_record(mut self, timestamp: u64, entry_id: u32) -> Self {
        let mut payload = Vec::new();
        payload.push(1); // Finish control type
        payload.write_u32::<LittleEndian>(entry_id).unwrap();

        self.write_record(0, timestamp, &payload);
        self
    }

    /// Add a Set Metadata control record
    pub fn set_metadata_record(mut self, timestamp: u64, entry_id: u32, metadata: &str) -> Self {
        let mut payload = Vec::new();
        payload.push(2); // Set Metadata control type
        payload.write_u32::<LittleEndian>(entry_id).unwrap();
        payload
            .write_u32::<LittleEndian>(metadata.len() as u32)
            .unwrap();
        payload.extend_from_slice(metadata.as_bytes());

        self.write_record(0, timestamp, &payload);
        self
    }

    /// Add a boolean record
    pub fn boolean_record(mut self, entry_id: u32, timestamp: u64, value: bool) -> Self {
        let payload = vec![if value { 1 } else { 0 }];
        self.write_record(entry_id, timestamp, &payload);
        self
    }

    /// Add an int64 record
    pub fn int64_record(mut self, entry_id: u32, timestamp: u64, value: i64) -> Self {
        let mut payload = Vec::new();
        payload.write_i64::<LittleEndian>(value).unwrap();
        self.write_record(entry_id, timestamp, &payload);
        self
    }

    /// Add a float record
    pub fn float_record(mut self, entry_id: u32, timestamp: u64, value: f32) -> Self {
        let mut payload = Vec::new();
        payload.write_f32::<LittleEndian>(value).unwrap();
        self.write_record(entry_id, timestamp, &payload);
        self
    }

    /// Add a double record
    pub fn double_record(mut self, entry_id: u32, timestamp: u64, value: f64) -> Self {
        let mut payload = Vec::new();
        payload.write_f64::<LittleEndian>(value).unwrap();
        self.write_record(entry_id, timestamp, &payload);
        self
    }

    /// Add a string record
    pub fn string_record(mut self, entry_id: u32, timestamp: u64, value: &str) -> Self {
        let payload = value.as_bytes().to_vec();
        self.write_record(entry_id, timestamp, &payload);
        self
    }

    /// Add a boolean array record
    pub fn boolean_array_record(
        mut self,
        entry_id: u32,
        timestamp: u64,
        values: &[bool],
    ) -> Self {
        let payload: Vec<u8> = values.iter().map(|&b| if b { 1 } else { 0 }).collect();
        self.write_record(entry_id, timestamp, &payload);
        self
    }

    /// Add an int64 array record
    pub fn int64_array_record(mut self, entry_id: u32, timestamp: u64, values: &[i64]) -> Self {
        let mut payload = Vec::new();
        for &val in values {
            payload.write_i64::<LittleEndian>(val).unwrap();
        }
        self.write_record(entry_id, timestamp, &payload);
        self
    }

    /// Add a float array record
    pub fn float_array_record(mut self, entry_id: u32, timestamp: u64, values: &[f32]) -> Self {
        let mut payload = Vec::new();
        for &val in values {
            payload.write_f32::<LittleEndian>(val).unwrap();
        }
        self.write_record(entry_id, timestamp, &payload);
        self
    }

    /// Add a double array record
    pub fn double_array_record(mut self, entry_id: u32, timestamp: u64, values: &[f64]) -> Self {
        let mut payload = Vec::new();
        for &val in values {
            payload.write_f64::<LittleEndian>(val).unwrap();
        }
        self.write_record(entry_id, timestamp, &payload);
        self
    }

    /// Add a string array record
    pub fn string_array_record(
        mut self,
        entry_id: u32,
        timestamp: u64,
        values: &[&str],
    ) -> Self {
        let mut payload = Vec::new();
        payload
            .write_u32::<LittleEndian>(values.len() as u32)
            .unwrap();
        for &s in values {
            payload.write_u32::<LittleEndian>(s.len() as u32).unwrap();
            payload.extend_from_slice(s.as_bytes());
        }
        self.write_record(entry_id, timestamp, &payload);
        self
    }

    /// Add a raw data record
    pub fn raw_record(mut self, entry_id: u32, timestamp: u64, data: &[u8]) -> Self {
        self.write_record(entry_id, timestamp, data);
        self
    }

    /// Add a struct schema record
    pub fn struct_schema_record(
        mut self,
        timestamp: u64,
        entry_id: u32,
        schema_name: &str,
        schema_def: &str,
    ) -> Self {
        self.start_record(
            timestamp,
            entry_id,
            &format!(".schema/{}", schema_name),
            "structschema",
            "",
        )
        .string_record(entry_id, timestamp, schema_def)
    }

    /// Add a struct data record
    pub fn struct_record(mut self, entry_id: u32, timestamp: u64, data: &[u8]) -> Self {
        self.write_record(entry_id, timestamp, data);
        self
    }

    /// Add a struct array data record
    pub fn struct_array_record(mut self, entry_id: u32, timestamp: u64, data: &[u8]) -> Self {
        self.write_record(entry_id, timestamp, data);
        self
    }

    /// Write a generic record with optimal variable-length encoding
    fn write_record(&mut self, entry_id: u32, timestamp: u64, payload: &[u8]) {
        // Determine optimal sizes
        let entry_len = Self::min_bytes_for_value(entry_id as u64);
        let size_len = Self::min_bytes_for_value(payload.len() as u64);
        let timestamp_len = Self::min_bytes_for_value(timestamp);

        // Write header byte
        let header_byte = (((entry_len - 1) & 0x3)
            | (((size_len - 1) & 0x3) << 2)
            | (((timestamp_len - 1) & 0x7) << 4)) as u8;
        self.data.push(header_byte);

        // Write entry ID
        Self::write_varint(&mut self.data, entry_id as u64, entry_len);

        // Write payload size
        Self::write_varint(&mut self.data, payload.len() as u64, size_len);

        // Write timestamp
        Self::write_varint(&mut self.data, timestamp, timestamp_len);

        // Write payload
        self.data.extend_from_slice(payload);
    }

    /// Determine minimum bytes needed to represent a value
    fn min_bytes_for_value(value: u64) -> usize {
        if value <= 0xFF {
            1
        } else if value <= 0xFFFF {
            2
        } else if value <= 0xFFFFFF {
            3
        } else if value <= 0xFFFFFFFF {
            4
        } else if value <= 0xFFFFFFFFFF {
            5
        } else if value <= 0xFFFFFFFFFFFF {
            6
        } else if value <= 0xFFFFFFFFFFFFFF {
            7
        } else {
            8
        }
    }

    /// Write a variable-length integer (little endian)
    fn write_varint(data: &mut Vec<u8>, value: u64, len: usize) {
        for i in 0..len {
            data.push(((value >> (i * 8)) & 0xFF) as u8);
        }
    }

    /// Build and return the final WPILOG data
    pub fn build(self) -> Vec<u8> {
        self.data
    }
}

impl Default for WpilogBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builder_creates_valid_header() {
        let data = WpilogBuilder::new().build();
        assert_eq!(&data[0..6], b"WPILOG");
        assert_eq!(data[6], 0x00); // Minor version
        assert_eq!(data[7], 0x01); // Major version
        assert_eq!(data[8..12], [0, 0, 0, 0]); // Extra header length = 0
    }

    #[test]
    fn test_builder_with_extra_header() {
        let data = WpilogBuilder::with_header(0x0100, "test").build();
        assert_eq!(&data[0..6], b"WPILOG");
        assert_eq!(data[6], 0x00); // Minor version
        assert_eq!(data[7], 0x01); // Major version
        assert_eq!(data[8], 4); // Extra header length
        assert_eq!(&data[12..16], b"test");
    }

    #[test]
    fn test_min_bytes_for_value() {
        assert_eq!(WpilogBuilder::min_bytes_for_value(0), 1);
        assert_eq!(WpilogBuilder::min_bytes_for_value(255), 1);
        assert_eq!(WpilogBuilder::min_bytes_for_value(256), 2);
        assert_eq!(WpilogBuilder::min_bytes_for_value(0xFFFF), 2);
        assert_eq!(WpilogBuilder::min_bytes_for_value(0x10000), 3);
        assert_eq!(WpilogBuilder::min_bytes_for_value(0xFFFFFFFF), 4);
        assert_eq!(WpilogBuilder::min_bytes_for_value(0x100000000), 5);
    }
}
