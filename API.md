# WPILog Parser Library API

A high-performance Rust library for parsing WPILib data log files and converting them to various formats.

## Table of Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
- [API Overview](#api-overview)
  - [Reading WPILog Files](#reading-wpilog-files)
  - [Writing Parquet Files](#writing-parquet-files)
  - [Error Handling](#error-handling)
- [Examples](#examples)
- [Advanced Usage](#advanced-usage)

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
wpilog-parser = "0.1"
```

## Quick Start

```rust
use wpilog_parser::{WpilogReader, ParquetWriter};

fn main() -> Result<(), wpilog_parser::Error> {
    // Read a WPILog file
    let reader = WpilogReader::from_file("data.wpilog")?;
    let records = reader.read_all()?;

    // Write to Parquet
    ParquetWriter::new("output_dir")
        .write(&records)?;

    Ok(())
}
```

## API Overview

### Reading WPILog Files

#### `WpilogReader`

The main entry point for reading WPILog files.

**Create from file:**
```rust
let reader = WpilogReader::from_file("data.wpilog")?;
```

**Create from bytes:**
```rust
let data: Vec<u8> = // ... load data
let reader = WpilogReader::from_bytes(data)?;
```

**Read all records:**
```rust
let records = reader.read_all()?;
```

**Read with metadata:**
```rust
let (records, formatter) = reader.read_all_with_metadata()?;

// Access metrics and schemas
println!("Metrics: {}", formatter.metrics_names.len());
println!("Schemas: {}", formatter.struct_schemas.len());
```

**Get file info:**
```rust
let version = reader.version();     // e.g., 0x0100
let header = reader.extra_header(); // UTF-8 string
```

**Low-level access:**
```rust
let low_level = reader.low_level_reader();

for record_result in low_level.records()? {
    let record = record_result?;
    // Process record manually
}
```

#### `WpilogReaderBuilder`

For advanced configuration:

```rust
use wpilog_parser::{WpilogReaderBuilder, OutputFormat};

let records = WpilogReaderBuilder::new()
    .output_format(OutputFormat::Wide)
    .from_file("data.wpilog")?
    .read()?;
```

### Writing Parquet Files

#### `ParquetWriter`

Write records to Apache Parquet format.

**Basic usage:**
```rust
ParquetWriter::new("output_directory")
    .write(&records)?;
```

**With chunk size:**
```rust
ParquetWriter::new("output_directory")
    .chunk_size(100_000)  // 100k rows per file
    .write(&records)?;
```

**With statistics:**
```rust
let stats = ParquetWriter::new("output_directory")
    .write_with_stats(&records)?;

println!("{}", stats.summary());
// Output: "Wrote 1,000,000 records across 10 file(s) (100,000 rows per file)"
```

#### `ParquetWriterBuilder`

For advanced configuration:

```rust
use wpilog_parser::ParquetWriterBuilder;

ParquetWriterBuilder::new()
    .output_directory("./output")
    .chunk_size(75_000)
    .build()?
    .write(&records)?;
```

### Error Handling

The library uses a custom `Error` type with comprehensive error variants:

```rust
use wpilog_parser::Error;

match WpilogReader::from_file("data.wpilog") {
    Ok(reader) => { /* ... */ }
    Err(Error::InvalidFormat(msg)) => {
        eprintln!("Invalid WPILOG file: {}", msg);
    }
    Err(Error::Io(err)) => {
        eprintln!("I/O error: {}", err);
    }
    Err(Error::ParseError(msg)) => {
        eprintln!("Parse error: {}", msg);
    }
    Err(err) => {
        eprintln!("Error: {}", err);
    }
}
```

**Error variants:**

- `InvalidFormat(String)` - Invalid WPILog file format
- `Io(std::io::Error)` - I/O errors
- `InvalidEntry(String)` - Invalid entry ID
- `ParseError(String)` - Data parsing errors
- `SchemaError(String)` - Schema inference errors
- `OutputError(String)` - Output format errors
- `Utf8Error(FromUtf8Error)` - UTF-8 encoding errors
- `Other(String)` - Generic errors

## Examples

### Simple Conversion

```rust
use wpilog_parser::{WpilogReader, ParquetWriter};

fn main() -> Result<(), wpilog_parser::Error> {
    let reader = WpilogReader::from_file("data.wpilog")?;
    let records = reader.read_all()?;

    ParquetWriter::new("output")
        .chunk_size(100_000)
        .write(&records)?;

    println!("Converted {} records", records.len());

    Ok(())
}
```

### Metadata Analysis

```rust
use wpilog_parser::WpilogReader;

fn main() -> Result<(), wpilog_parser::Error> {
    let reader = WpilogReader::from_file("data.wpilog")?;
    let (records, formatter) = reader.read_all_with_metadata()?;

    println!("File version: {:#06x}", reader.version());
    println!("Records: {}", records.len());
    println!("Unique metrics: {}", formatter.metrics_names.len());

    // List all metrics
    for name in &formatter.metrics_names {
        println!("  - {}", name);
    }

    // Examine struct schemas
    for schema in &formatter.struct_schemas {
        println!("Schema: {}", schema.name);
        for col in &schema.columns {
            println!("  {}: {}", col.name, col.type_name);
        }
    }

    Ok(())
}
```

### Custom Processing

```rust
use wpilog_parser::WpilogReader;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let reader = WpilogReader::from_file("data.wpilog")?;
    let records = reader.read_all()?;

    // Find all double values from a specific metric
    let temperatures: Vec<f64> = records
        .iter()
        .filter_map(|r| r.data.get("/sensor/temperature"))
        .filter_map(|v| v.as_f64())
        .collect();

    let avg = temperatures.iter().sum::<f64>() / temperatures.len() as f64;
    println!("Average temperature: {:.2}", avg);

    Ok(())
}
```

### Error Handling Example

```rust
use wpilog_parser::{WpilogReader, Error};
use std::path::Path;

fn safe_parse(path: &Path) -> Result<usize, Error> {
    let reader = WpilogReader::from_file(path)?;
    let records = reader.read_all()?;
    Ok(records.len())
}

fn main() {
    match safe_parse(Path::new("data.wpilog")) {
        Ok(count) => println!("Parsed {} records", count),
        Err(Error::InvalidFormat(msg)) => {
            eprintln!("Not a valid WPILOG file: {}", msg);
        }
        Err(Error::Io(err)) if err.kind() == std::io::ErrorKind::NotFound => {
            eprintln!("File not found");
        }
        Err(err) => {
            eprintln!("Unexpected error: {}", err);
        }
    }
}
```

## Advanced Usage

### Accessing Low-Level API

For performance-critical applications or custom parsing logic:

```rust
use wpilog_parser::WpilogReader;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let reader = WpilogReader::from_file("data.wpilog")?;
    let low_level = reader.low_level_reader();

    for record_result in low_level.records()? {
        let record = record_result?;

        if record.is_control() {
            if record.is_start() {
                let start_data = record.get_start_data()?;
                println!("Started entry: {}", start_data.name);
            }
        } else {
            // Process data record
            match record.entry {
                1 => {
                    let value = record.get_double()?;
                    println!("Got double: {}", value);
                }
                _ => {}
            }
        }
    }

    Ok(())
}
```

### Data Model

#### `WideRow`

Represents a single timestamped record with all metric values:

```rust
pub struct WideRow {
    pub timestamp: f64,          // Time in seconds
    pub entry: u32,              // Entry ID
    pub type_name: String,       // Type string (e.g., "double")
    pub loop_count: u64,         // Loop iteration number
    pub data: HashMap<String, serde_json::Value>,  // Metric values
}
```

Access data:
```rust
if let Some(value) = record.data.get("/sensor/temperature") {
    if let Some(temp) = value.as_f64() {
        println!("Temperature: {}", temp);
    }
}
```

#### Supported Data Types

| WPILog Type | Rust Type | Example |
|-------------|-----------|---------|
| `boolean` | `bool` | `value.as_bool()` |
| `int64` | `i64` | `value.as_i64()` |
| `float` | `f64` | `value.as_f64()` |
| `double` | `f64` | `value.as_f64()` |
| `string` | `String` | `value.as_str()` |
| `boolean[]` | `Vec<bool>` | `value.as_array()` |
| `int64[]` | `Vec<i64>` | `value.as_array()` |
| `float[]` | `Vec<f32>` | `value.as_array()` |
| `double[]` | `Vec<f64>` | `value.as_array()` |
| `string[]` | `Vec<String>` | `value.as_array()` |

#### `WriteStats`

Statistics about a Parquet write operation:

```rust
pub struct WriteStats {
    pub num_records: usize,  // Total records written
    pub num_chunks: usize,   // Number of files created
    pub chunk_size: usize,   // Rows per file
}
```

## Performance Tips

1. **Use `read_all()` for simple cases**: Lowest overhead for typical usage
2. **Use `read_all_with_metadata()` when you need metrics list**
3. **Use low-level API for streaming**: Avoids loading all data into memory
4. **Adjust chunk size**: Larger chunks = fewer files but more memory
5. **Enable release mode**: `cargo build --release` for ~10x speedup

## Thread Safety

- `WpilogReader` is `Send` but not `Sync` (use per-thread)
- `ParquetWriter` is `Send` but not `Sync` (use per-thread)
- Reading multiple files in parallel is safe and recommended

Example parallel processing:

```rust
use rayon::prelude::*;
use wpilog_parser::{WpilogReader, ParquetWriter};

fn process_files(files: Vec<&str>) -> Result<(), Box<dyn std::error::Error>> {
    files.par_iter().try_for_each(|&file| {
        let reader = WpilogReader::from_file(file)?;
        let records = reader.read_all()?;

        let output = format!("output/{}", file);
        ParquetWriter::new(&output).write(&records)?;

        Ok::<(), Box<dyn std::error::Error>>(())
    })?;

    Ok(())
}
```

## CLI Usage

The library includes a CLI tool:

```bash
# Convert all .wpilog files in a directory
wpilog-parser /path/to/logs --out-root ./output

# With custom chunk size
wpilog-parser /path/to/logs --out-root ./output --chunk-size 100000
```

## License

See LICENSE file in the repository.
