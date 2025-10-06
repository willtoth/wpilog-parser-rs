use anyhow::{anyhow, Result};
use byteorder::{LittleEndian, ReadBytesExt};
use std::io::Cursor;

const CONTROL_START: u8 = 0;
const CONTROL_FINISH: u8 = 1;
const CONTROL_SET_METADATA: u8 = 2;

#[derive(Debug, Clone)]
pub struct StartRecordData {
    pub entry: u32,
    pub name: String,
    pub type_name: String,
    pub metadata: String,
}

#[derive(Debug, Clone)]
pub struct MetadataRecordData {
    pub entry: u32,
    pub metadata: String,
}

#[derive(Debug, Clone)]
pub struct DataLogRecord {
    pub entry: u32,
    pub timestamp: u64,
    pub data: Vec<u8>,
}

impl DataLogRecord {
    pub fn is_control(&self) -> bool {
        self.entry == 0
    }

    fn get_control_type(&self) -> Option<u8> {
        self.data.first().copied()
    }

    pub fn is_start(&self) -> bool {
        self.entry == 0
            && self.data.len() >= 17
            && self.get_control_type() == Some(CONTROL_START)
    }

    pub fn is_finish(&self) -> bool {
        self.entry == 0
            && self.data.len() == 5
            && self.get_control_type() == Some(CONTROL_FINISH)
    }

    pub fn is_set_metadata(&self) -> bool {
        self.entry == 0
            && self.data.len() >= 9
            && self.get_control_type() == Some(CONTROL_SET_METADATA)
    }

    pub fn get_start_data(&self) -> Result<StartRecordData> {
        if !self.is_start() {
            return Err(anyhow!("Not a start record"));
        }

        let mut cursor = Cursor::new(&self.data);
        cursor.set_position(1); // Skip control type

        let entry = cursor.read_u32::<LittleEndian>()?;
        let (name, pos) = read_inner_string(&self.data, cursor.position() as usize)?;
        let (type_name, pos) = read_inner_string(&self.data, pos)?;
        let (metadata, _) = read_inner_string(&self.data, pos)?;

        Ok(StartRecordData {
            entry,
            name,
            type_name,
            metadata,
        })
    }

    pub fn get_finish_entry(&self) -> Result<u32> {
        if !self.is_finish() {
            return Err(anyhow!("Not a finish record"));
        }

        let mut cursor = Cursor::new(&self.data[1..5]);
        Ok(cursor.read_u32::<LittleEndian>()?)
    }

    pub fn get_set_metadata_data(&self) -> Result<MetadataRecordData> {
        if !self.is_set_metadata() {
            return Err(anyhow!("Not a set metadata record"));
        }

        let mut cursor = Cursor::new(&self.data[1..5]);
        let entry = cursor.read_u32::<LittleEndian>()?;
        let (metadata, _) = read_inner_string(&self.data, 5)?;

        Ok(MetadataRecordData { entry, metadata })
    }

    pub fn get_boolean(&self) -> Result<bool> {
        if self.data.len() != 1 {
            return Err(anyhow!("Not a boolean"));
        }
        Ok(self.data[0] != 0)
    }

    pub fn get_integer(&self) -> Result<i64> {
        if self.data.len() != 8 {
            return Err(anyhow!("Not an integer"));
        }
        let mut cursor = Cursor::new(&self.data);
        Ok(cursor.read_i64::<LittleEndian>()?)
    }

    pub fn get_float(&self) -> Result<f32> {
        if self.data.len() != 4 {
            return Err(anyhow!("Not a float"));
        }
        let mut cursor = Cursor::new(&self.data);
        Ok(cursor.read_f32::<LittleEndian>()?)
    }

    pub fn get_double(&self) -> Result<f64> {
        if self.data.len() != 8 {
            return Err(anyhow!("Not a double"));
        }
        let mut cursor = Cursor::new(&self.data);
        Ok(cursor.read_f64::<LittleEndian>()?)
    }

    pub fn get_string(&self) -> Result<String> {
        String::from_utf8(self.data.clone())
            .map_err(|e| anyhow!("Invalid UTF-8: {}", e))
    }

    pub fn get_msgpack(&self) -> Result<rmpv::Value> {
        rmpv::decode::read_value(&mut Cursor::new(&self.data))
            .map_err(|e| anyhow!("MsgPack decode error: {}", e))
    }

    pub fn get_boolean_array(&self) -> Vec<bool> {
        self.data.iter().map(|&x| x != 0).collect()
    }

    pub fn get_integer_array(&self) -> Result<Vec<i64>> {
        if self.data.len() % 8 != 0 {
            return Err(anyhow!("Not an integer array"));
        }
        let mut result = Vec::with_capacity(self.data.len() / 8);
        let mut cursor = Cursor::new(&self.data);
        while cursor.position() < self.data.len() as u64 {
            result.push(cursor.read_i64::<LittleEndian>()?);
        }
        Ok(result)
    }

    pub fn get_float_array(&self) -> Result<Vec<f32>> {
        if self.data.len() % 4 != 0 {
            return Err(anyhow!("Not a float array"));
        }
        let mut result = Vec::with_capacity(self.data.len() / 4);
        let mut cursor = Cursor::new(&self.data);
        while cursor.position() < self.data.len() as u64 {
            result.push(cursor.read_f32::<LittleEndian>()?);
        }
        Ok(result)
    }

    pub fn get_double_array(&self) -> Result<Vec<f64>> {
        if self.data.len() % 8 != 0 {
            return Err(anyhow!("Not a double array"));
        }
        let mut result = Vec::with_capacity(self.data.len() / 8);
        let mut cursor = Cursor::new(&self.data);
        while cursor.position() < self.data.len() as u64 {
            result.push(cursor.read_f64::<LittleEndian>()?);
        }
        Ok(result)
    }

    pub fn get_string_array(&self) -> Result<Vec<String>> {
        let mut cursor = Cursor::new(&self.data);
        let size = cursor.read_u32::<LittleEndian>()? as usize;

        if size > (self.data.len() - 4) / 4 {
            return Err(anyhow!("Not a string array"));
        }

        let mut result = Vec::with_capacity(size);
        let mut pos = 4;

        for _ in 0..size {
            let (s, new_pos) = read_inner_string(&self.data, pos)?;
            result.push(s);
            pos = new_pos;
        }

        Ok(result)
    }
}

fn read_inner_string(data: &[u8], pos: usize) -> Result<(String, usize)> {
    if pos + 4 > data.len() {
        return Err(anyhow!("Invalid string size position"));
    }

    let mut cursor = Cursor::new(&data[pos..pos + 4]);
    let size = cursor.read_u32::<LittleEndian>()? as usize;
    let end = pos + 4 + size;

    if end > data.len() {
        return Err(anyhow!("Invalid string size"));
    }

    let s = String::from_utf8(data[pos + 4..end].to_vec())
        .map_err(|e| anyhow!("Invalid UTF-8 in string: {}", e))?;

    Ok((s, end))
}

pub struct DataLogReader<'a> {
    data: &'a [u8],
}

impl<'a> DataLogReader<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data }
    }

    pub fn is_valid(&self) -> bool {
        self.data.len() >= 12
            && &self.data[0..6] == b"WPILOG"
            && self.get_version() >= 0x0100
    }

    pub fn get_version(&self) -> u16 {
        if self.data.len() < 12 {
            return 0;
        }
        let mut cursor = Cursor::new(&self.data[6..8]);
        cursor.read_u16::<LittleEndian>().unwrap_or(0)
    }

    pub fn get_extra_header(&self) -> String {
        if self.data.len() < 12 {
            return String::new();
        }

        let mut cursor = Cursor::new(&self.data[8..12]);
        let size = cursor.read_u32::<LittleEndian>().unwrap_or(0) as usize;

        if 12 + size > self.data.len() {
            return String::new();
        }

        String::from_utf8(self.data[12..12 + size].to_vec()).unwrap_or_default()
    }

    pub fn records(&self) -> Result<DataLogIterator<'a>> {
        if !self.is_valid() {
            return Err(anyhow!("Not a valid WPILOG file"));
        }

        let mut cursor = Cursor::new(&self.data[8..12]);
        let extra_header_size = cursor.read_u32::<LittleEndian>()? as usize;
        let start_pos = 12 + extra_header_size;

        Ok(DataLogIterator {
            data: self.data,
            pos: start_pos,
        })
    }
}

pub struct DataLogIterator<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Iterator for DataLogIterator<'a> {
    type Item = Result<DataLogRecord>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.data.len() < self.pos + 4 {
            return None;
        }

        let header_byte = self.data[self.pos];
        let entry_len = ((header_byte & 0x3) + 1) as usize;
        let size_len = (((header_byte >> 2) & 0x3) + 1) as usize;
        let timestamp_len = (((header_byte >> 4) & 0x7) + 1) as usize;
        let header_len = 1 + entry_len + size_len + timestamp_len;

        if self.data.len() < self.pos + header_len {
            return None;
        }

        let entry = read_varint(&self.data[self.pos + 1..], entry_len);
        let size = read_varint(&self.data[self.pos + 1 + entry_len..], size_len) as usize;
        let timestamp = read_varint(&self.data[self.pos + 1 + entry_len + size_len..], timestamp_len);

        if self.data.len() < self.pos + header_len + size {
            return None;
        }

        let data = self.data[self.pos + header_len..self.pos + header_len + size].to_vec();

        let record = DataLogRecord {
            entry: entry as u32,
            timestamp,
            data,
        };

        self.pos += header_len + size;

        Some(Ok(record))
    }
}

fn read_varint(data: &[u8], len: usize) -> u64 {
    let mut val = 0u64;
    for i in 0..len {
        val |= (data[i] as u64) << (i * 8);
    }
    val
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_varint() {
        let data = vec![0x01, 0x00, 0x00, 0x00];
        assert_eq!(read_varint(&data, 1), 1);
        assert_eq!(read_varint(&data, 4), 1);
    }
}
