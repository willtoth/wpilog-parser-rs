import mmap
import json
import re

from typing import List, Dict, Any, Union, Set

from wpilib.datalog import DataLogReader, DataLogRecord, StartRecordData
from format.models import (
    OutputFormat,
    WideRow,
    LongRow,
    NestedValue
)

def sanitize_column_name(name: str) -> str:
    """Sanitizes column names by replacing invalid characters with underscores."""
    return re.sub(r"[^a-zA-Z0-9]+", "_", name).strip("_")  # Replace groups of special characters with "_"

class Formatter:
    def __init__(self,
                 wpilog_file: str,
                 output_file: str,
                 output_format: OutputFormat = OutputFormat.WIDE) -> None:

        self.wpilog_file: str = wpilog_file
        self.output_file: str = output_file
        self.output_format = output_format
        self.parse_methods = {
            OutputFormat.WIDE: self.parse_record_wide,
            OutputFormat.LONG: self.parse_record_long
        }
        self.metrics_names: Set[str] = set()

    @staticmethod
    def parse_record_wide(record: DataLogRecord,
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
            print("Name: " + entry.name)
            print(type(record.data))
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
            parsed_data[sanitize_column_name(entry.name)] = record.data.__bytes__()
        else:
            parsed_data[sanitize_column_name(entry.name)] = record.data.__bytes__()
        return WideRow(
            **parsed_data
        )

    @staticmethod
    def parse_record_long(record: DataLogRecord,
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
                row.value.boolean_array= record.getBooleanArray()
            elif entry.type == "double[]":
                row.value.double_array = list(record.getDoubleArray())
            elif entry.type == "float[]":
                row.value.float_array = list(record.getFloatArray())
            elif entry.type == "int64[]":
                row.value.int64_array = list(record.getIntegerArray())
            elif entry.type == "string[]":
                row.value.string_array= record.getStringArray()
        except TypeError:
            row.value = None

        return row

    def read_wpilog(self) -> List[Union[WideRow, LongRow]]:
        """Reads the WPILOG file and processes records into a structured format."""
        records: List[Union[WideRow, LongRow]]  = []
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
                        parsed_data: Union[WideRow, LongRow] = parse_method(record, entry)
                        self.metrics_names.add(entry.name)
                        records.append(parsed_data)
        return records

    def convert(self,
                rows: List[Union[WideRow, LongRow]]):
        pass