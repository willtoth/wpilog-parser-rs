from enum import Enum
from typing import List, Dict, Optional, Any
from pydantic import BaseModel, Extra, Field

class FileFormat(Enum):
    PARQUET = 'parquet'
    AVRO = 'avro'
    JSON = 'json'

class OutputFormat(Enum):
    WIDE = 'wide'
    LONG = 'long'

class WideRow(BaseModel, extra=Extra.allow):
    """
    A wide row will will have all entity.names as columns, with their respective values.
    this means we never truly fully know all of the columns in this output until you have the file.
    """
    timestamp: float
    entry: int
    type: str

class NestedValue(BaseModel):
    double: Optional[float]
    int64: Optional[int]
    string: Optional[str]
    boolean: Optional[bool]
    boolean_array: Optional[List[bool]]
    double_array: Optional[List[float]]
    float_array: Optional[List[float]]
    int64_array: Optional[List[int]]
    string_array: Optional[List[str]]

class LongRow(BaseModel):
    timestamp: float
    entry: int
    type: str
    json: Optional[Dict[Any, Any]]
    value: Optional[NestedValue]
