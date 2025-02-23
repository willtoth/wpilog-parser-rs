import pandas as pd

from typing import List, Union
from typing_extensions import override

from format.formatter import Formatter
from format.models import WideRow, LongRow, OutputFormat



class FormatParquet(Formatter):
    def __init__(self, wpilog_file: str,
                 output_file: str,
                 output_format: OutputFormat = OutputFormat.WIDE):
        super().__init__(wpilog_file, output_file, output_format)


    @override
    def convert(self,
                rows: List[Union[WideRow, LongRow]]):
        if not rows:
            raise ValueError(f"No valid records to write to Parquet for {self.output_file}")

        with open(self.output_file, 'x') as file:
            df: pd.DataFrame = pd.DataFrame([row.model_dump() for row in rows])
            print(f"Total Columns being written to parquet file: {len(df.columns)}")
            df.to_parquet(self.output_file)


