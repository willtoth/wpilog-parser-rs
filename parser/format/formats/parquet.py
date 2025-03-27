import pandas as pd
import math
import os

from typing import List, Union
from typing_extensions import override

from format.formatter import Formatter
from format.models import WideRow, LongRow, OutputFormat


class FormatParquet(Formatter):
    def __init__(self, wpilog_file: str,
                 output_directory: str,
                 output_format: OutputFormat = OutputFormat.WIDE):
        super().__init__(wpilog_file, output_directory, output_format)

    @override
    def convert(self, rows: List[Union[WideRow, LongRow]], chunk_size: int = 50_000):
        if not rows:
            raise ValueError(f"No valid records to write to Parquet for {self.output_directory}")

        os.makedirs(self.output_directory, exist_ok=True)
        
        total_chunks = math.ceil(len(rows) / chunk_size)

        print(f"Generated a total of {total_chunks}, will now create that total amount of files.")
        for i in range(total_chunks):
            chunk = rows[i * chunk_size: (i + 1) * chunk_size]
            df = pd.DataFrame([row.model_dump() for row in chunk])
            print(f"Writing chunk {i + 1}/{total_chunks}, {len(df)} rows, {len(df.columns)} columns")
            df.to_parquet(f"{self.output_directory}/file_part{i:03}.parquet")

        print("all chunks have been written")
