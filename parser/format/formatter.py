import mmap
import json

from typing import List, Dict, Any, Union

from wpilib.datalog import DataLogReader, DataLogRecord, StartRecordData
from format.models import (
    OutputFormat,
    WideRow,
    LongRow,
    NestedValue
)

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

    @staticmethod
    def parse_record_wide(record: DataLogRecord,
                          entry: StartRecordData) -> WideRow:
        """Parses a WPILOG record into a structured dictionary with flattened data."""
        parsed_data: Dict[str, Any] = {
            "timestamp": record.timestamp / 1_000_000,  # Convert to seconds
            "entry": record.entry,
            "type": entry.type
        }

        try:
            if entry.type == "double":
                parsed_data[entry.name] = record.getDouble()
            elif entry.type == "int64":
                parsed_data[entry.name] = record.getInteger()
            elif entry.type in ("string", "json"):
                parsed_data[entry.name] = record.getString()
            elif entry.type == "boolean":
                parsed_data[entry.name] = record.getBoolean()
            elif entry.type == "boolean[]":
                parsed_data[entry.name] = record.getBooleanArray()
            elif entry.type == "double[]":
                parsed_data[entry.name] = list(record.getDoubleArray())
            elif entry.type == "float[]":
                parsed_data[entry.name] = list(record.getFloatArray())
            elif entry.type == "int64[]":
                parsed_data[entry.name] = list(record.getIntegerArray())
            elif entry.type == "string[]":
                parsed_data[entry.name] = record.getStringArray()
        except TypeError:
            parsed_data[entry.name] = None

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
                    except TypeError:
                        continue
                elif record.isFinish():
                    try:
                        entry: int = record.getFinishEntry()
                        entries.pop(entry, None)
                    except TypeError:
                        continue
                elif not record.isControl():
                    entry: Union[StartRecordData, None] = entries.get(record.entry)
                    if entry:
                        parse_method = self.parse_methods.get(self.output_format)
                        parsed_data: Union[WideRow, LongRow] = parse_method(record, entry)
                        records.append(parsed_data)
        return records

    def convert(self,
                rows: List[Union[WideRow, LongRow]]):
        pass