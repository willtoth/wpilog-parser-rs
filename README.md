# WPILog Parser (Rust)

A high-performance Rust library for parsing WPILib data log files and converting them to Parquet format.

## AI Usage

This library is a fork of https://github.com/agaddis02/wpilog-parser using Claude Sonnet 4.5 as a translator. This has not been fully vetted, and is being encorporated for a specific project. Use at your own risk.

## Features

- Fast, memory-efficient parsing of WPILib data log files
- Support for all WPILog data types (boolean, int64, double, float, string, arrays, msgpack, structs)
- Parquet output format with chunking for large files
- Wide and long output formats
- Struct schema parsing and unpacking
- Memory-mapped file access for efficient reading
- Clean library API for use in other Rust projects

## Using as a Library

Add to your `Cargo.toml`:

```toml
[dependencies]
# Local path dependency (development):
wpilog-parser = { path = "/path/to/wpilog-parser" }

# Or Git dependency (production/CI):
wpilog-parser = { git = "https://github.com/willtoth/wpilog-parser-rs" }
```

Example usage:

```rust
use wpilog_parser::{WpilogReader, ParquetWriter};

fn main() -> Result<(), wpilog_parser::Error> {
    // Read WPILog file
    let reader = WpilogReader::from_file("data.wpilog")?;
    let records = reader.read_all()?;

    // Write to Parquet
    ParquetWriter::new("output_dir")
        .chunk_size(100_000)
        .write(&records)?;

    Ok(())
}
```

See [API.md](API.md) for complete API documentation.

## Building

```bash
cargo build --release
```

## CLI Usage

Parse all `.wpilog` files in a directory:

```bash
cargo run --release -- <INPUT_DIR> --out-root <OUTPUT_DIR>
```

### Options

- `<INPUT_DIR>`: Directory containing `.wpilog` files (required)
- `--out-root <OUTPUT_DIR>`: Root output directory for converted files (required)
- `--file-format <FORMAT>`: Output file format (default: `parquet`)
  - `parquet`: Apache Parquet format
  - `avro`: Apache Avro format (not yet implemented)
  - `json`: JSON format (not yet implemented)
- `--output-format <FORMAT>`: Output data format (default: `wide`)
  - `wide`: Wide format with each metric as a column
  - `long`: Long format with nested values (not fully implemented)

### Example

```bash
cargo run --release -- ./input-logs --out-root ./output --file-format parquet --output-format wide
```

## Architecture

- `src/datalog.rs`: Core binary parser for WPILog format
- `src/models.rs`: Data structures and schema definitions
- `src/formatter.rs`: Record parsing and transformation logic
- `src/formats/parquet.rs`: Parquet output writer
- `src/main.rs`: CLI entry point

## Output

The parser creates a directory for each input `.wpilog` file, containing chunked Parquet files (50,000 rows per chunk by default).

Output directory structure:
```
output/
├── log1/
│   ├── file_part000.parquet
│   ├── file_part001.parquet
│   └── ...
└── log2/
    ├── file_part000.parquet
    └── ...
```

## Performance

The Rust implementation provides significant performance improvements over the Python version:
- Zero-copy parsing with memory-mapped files
- No GIL limitations
- Efficient memory usage
- Fast Parquet writing with Apache Arrow

## Testing

Run the comprehensive test suite:

```bash
cargo test
```

**92 tests** covering:
- Binary format parsing (49 tests)
- Full parsing pipeline (21 tests)
- Parquet schema types (8 tests)
- Documentation examples (13 tests)
- Common utilities (1 test)

## Documentation

- [API.md](API.md) - Complete library API documentation
- [STRUCT_PARSING.md](STRUCT_PARSING.md) - Struct parsing implementation details
- [TESTING.md](TESTING.md) - Test suite documentation (if exists)

## License

This project is based on WPILib's data log format. Original Python reference implementation is archived in `archive/parser-python/`.
