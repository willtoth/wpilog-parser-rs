import mmap
import json
import struct
import re

from typing import List, Dict, Any, Union, Set, Optional, Tuple

from wpilib.datalog import DataLogReader, DataLogRecord, StartRecordData
from format.models import (
    OutputFormat,
    WideRow,
    LongRow,
    NestedValue,
    DerivedSchemaColumn,
    DerivedSchema
)


def convert_struct_schema_to_dict(schema_str: str) -> List[DerivedSchemaColumn]:
    fields = []
    # Split by semicolon, then extract type and name
    for part in schema_str.split(';'):
        part = part.strip()
        if not part:
            continue
        # Handle enum inline
        if part.startswith("enum"):
            # Example: enum {MEGATAG_1=0, MEGATAG_2=1, PHOTONVISION=2} int32 type
            enum_part, type_and_name = part.split('}', 1)
            type_and_name = type_and_name.strip()
            if ' ' in type_and_name:
                typ, name = type_and_name.split()
                fields.append((typ, name))
        else:
            if ' ' in part:
                typ, name = part.split()
                fields.append((typ, name))

    columns: List[DerivedSchemaColumn] = []
    for type, name in fields:
        columns.append(DerivedSchemaColumn(name=name, type=type))

    return columns


def sanitize_column_name(name: str) -> str:
    """Sanitizes column names by replacing invalid characters with underscores."""
    return name  # re.sub(r"[^a-zA-Z0-9]+", "_", name).strip("_")  # Replace groups of special characters with "_"


class Formatter:
    def __init__(self,
                 wpilog_file: str,
                 output_directory: str,
                 output_format: OutputFormat = OutputFormat.WIDE) -> None:

        self.wpilog_file: str = wpilog_file
        self.output_directory: str = output_directory
        self.output_format = output_format
        self.parse_methods = {
            OutputFormat.WIDE: self.parse_record_wide,
            OutputFormat.LONG: self.parse_record_long
        }
        self.metrics_names: Set[str] = set()
        self.current_entry_type: Optional[str] = None
        self.struct_schemas: List[DerivedSchema] = []

    def parse_record_wide(self, record: DataLogRecord,
                          entry: StartRecordData) -> WideRow:
        """Parses a WPILOG record into a structured dictionary with flattened data."""
        parsed_data: Dict[str, Any] = {
            "timestamp": record.timestamp / 1_000_000,  # Convert to seconds
            "entry": record.entry,
            "type": entry.type
        }

        # try:
        if entry.type == "double":
            parsed_data[sanitize_column_name(entry.name)] = record.getDouble()
        elif entry.type == "int64":
            parsed_data[sanitize_column_name(entry.name)] = record.getInteger()
        elif entry.type in ("string", "json"):
            # print("Name: " + entry.name)
            # print(type(record.data))
            parsed_data[sanitize_column_name(entry.name)] = record.getString()
        elif entry.type == "boolean":
            parsed_data[sanitize_column_name(entry.name)] = record.getBoolean()
        elif entry.type == "boolean[]":
            parsed_data[sanitize_column_name(entry.name)] = record.getBooleanArray()
        elif entry.type == "double[]":
            parsed_data[sanitize_column_name(entry.name)] = list(record.getDoubleArray())
        elif entry.type == "float[]":
            parsed_data[sanitize_column_name(entry.name)] = list(record.getFloatArray())
        elif entry.type == "int64[]":
            parsed_data[sanitize_column_name(entry.name)] = list(record.getIntegerArray())
        elif entry.type == "string[]":
            parsed_data[sanitize_column_name(entry.name)] = record.getStringArray()
        elif entry.type == "msgpack":
            parsed_data[sanitize_column_name(entry.name)] = record.getMsgPack()
        elif "proto" in entry.type:
            parsed_data[sanitize_column_name(entry.name)] = record.data.__bytes__()
        elif entry.type == 'structschema':
            # print(f"Storing struct: {entry.name}, with schema of: {record.getString()}")
            columns: List[DerivedSchemaColumn] = convert_struct_schema_to_dict(record.getString())
            schema_name: str = entry.name.split(".schema/")[1]
            derived_schema: DerivedSchema = DerivedSchema(
                name=schema_name,
                columns=columns
            )
            # print(f"Derived Schema: {derived_schema}")
            self.struct_schemas.append(derived_schema)
            parsed_data[sanitize_column_name(entry.name)] = record.data.__bytes__()

        elif entry.type.startswith('struct:'):
            schema_name = entry.type.split('[]')[0] if entry.type.endswith('[]') else entry.type
            # print(f"Schema name: {schema_name}")
            struct_data = record.data.__bytes__()

            # Find schema
            schema = next((s for s in self.struct_schemas if s.name == schema_name), None)
            if schema is None:
                raise ValueError(f"No struct schema found for: {schema_name}")

            def unpack_struct(columns: List[DerivedSchemaColumn], data: bytes, offset: int = 0, prefix: str = "") -> \
                    Tuple[Dict[str, Any], int]:
                result = {}
                for col in columns:
                    key = f"{col.name}" if prefix else col.name
                    if col.type == "double":
                        # print(key)
                        result[key] = struct.unpack_from("<d", data, offset)[0] if data else None
                        offset += 8
                    elif col.type == "float":
                        result[key] = struct.unpack_from("<f", data, offset)[0] if data else None
                        offset += 4
                    elif col.type == "int32":
                        result[key] = struct.unpack_from("<i", data, offset)[0] if data else None
                        offset += 4
                    elif col.type == "int64":
                        result[key] = struct.unpack_from("<q", data, offset)[0] if data else None
                        offset += 8
                    else:
                        # Recurse into nested struct
                        nested_schema = next((s for s in self.struct_schemas if s.name.split('struct:')[1] == col.type),
                                             None)
                        if not nested_schema:
                            raise ValueError(f"No nested schema found for: {col.type}")
                        nested_result, offset = unpack_struct(nested_schema.columns, data, offset, key)
                        result.update(nested_result)
                return result, offset

            struct_values, _ = unpack_struct(schema.columns, struct_data)
            parsed_data[entry.name] = struct_values
        else:
            parsed_data[sanitize_column_name(entry.name)] = record.data.__bytes__()
        return WideRow(
            **parsed_data
        )

    def parse_record_long(self, record: DataLogRecord,
                          entry: StartRecordData) -> LongRow:
        """Parses a WPILOG record into a structured dictionary with flattened data."""
        row: LongRow = (
            LongRow(
                timestamp=record.timestamp / 1_000_000,
                entry=record.entry,
                type=entry.type,
                json=dict(),
                value=NestedValue(
                    double=None,
                    int64=None,
                    string=None,
                    boolean=None,
                    boolean_array=None,
                    double_array=None,
                    float_array=None,
                    int64_array=None,
                    string_array=None
                )
            )
        )
        try:
            if entry.type == "double":
                row.value.double = record.getDouble()
            elif entry.type == "int64":
                row.value.int64 = record.getInteger()
            elif entry.type == "string":
                row.value.string = record.getString()
            elif entry.type == "json":
                row.json = json.loads(record.getString())
            elif entry.type == "boolean":
                row.value.boolean = record.getBoolean()
            elif entry.type == "boolean[]":
                row.value.boolean_array = record.getBooleanArray()
            elif entry.type == "double[]":
                row.value.double_array = list(record.getDoubleArray())
            elif entry.type == "float[]":
                row.value.float_array = list(record.getFloatArray())
            elif entry.type == "int64[]":
                row.value.int64_array = list(record.getIntegerArray())
            elif entry.type == "string[]":
                row.value.string_array = record.getStringArray()
        except TypeError:
            row.value = None

        return row

    def read_wpilog(self, infer_schema_only: bool = False) -> List[Union[WideRow, LongRow]]:
        """Reads the WPILOG file and processes records into a structured format."""
        records: List[Union[WideRow, LongRow]] = []
        entries: Dict[int, StartRecordData] = {}

        with open(self.wpilog_file, "rb") as f:
            mm: mmap.mmap = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
            reader: DataLogReader = DataLogReader(mm)

            if not reader:
                raise ValueError("Not a valid WPILOG file")

            for record in reader:
                if record.isStart():
                    try:
                        data: StartRecordData = record.getStartData()
                        entries[data.entry] = data
                    except TypeError as error:
                        raise Exception(error)
                elif record.isFinish():
                    try:
                        entry: int = record.getFinishEntry()
                        entries.pop(entry, None)
                    except TypeError as error:
                        raise Exception(error)

                elif not record.isControl():
                    entry: Union[StartRecordData, None] = entries.get(record.entry)
                    if entry:
                        parse_method = self.parse_methods.get(self.output_format)
                        if infer_schema_only:
                            if entry.type == 'structschema':
                                # print(f"Storing struct: {entry.name}, with schema of: {record.getString()}")
                                columns: List[DerivedSchemaColumn] = convert_struct_schema_to_dict(record.getString())
                                schema_name: str = entry.name.split(".schema/")[1]
                                derived_schema: DerivedSchema = DerivedSchema(
                                    name=schema_name,
                                    columns=columns
                                )
                                # print(f"Derived Schema: {derived_schema}")
                                self.struct_schemas.append(derived_schema)
                        else:
                            parsed_data: Union[WideRow, LongRow] = self.parse_record_wide(record, entry)
                            self.current_entry_type = entry.type
                            # print(self.current_entry_type)
                            self.metrics_names.add(entry.name)
                            records.append(parsed_data)
        return records

    def convert(self,
                rows: List[Union[WideRow, LongRow]]):
        pass
