import logging
import os
from typing import Optional

import typer

from format.models import OutputFormat, FileFormat
from format.formats.parquet import FormatParquet

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def parse_file(
        in_file: str,
        file_format: FileFormat,
        output_format: OutputFormat = OutputFormat.LONG,
        out_file: str | None = None,
):
    if not out_file:
        out_file = f"{in_file.split('.wpilog')[0].parquet}"

    logger.info(f"Attempting to transform {in_file} —> {out_file}")
    match file_format:
        case FileFormat.AVRO:
            raise NotImplemented()
        case FileFormat.PARQUET:
            converter: FormatParquet = FormatParquet(wpilog_file=in_file,
                                                     output_file=out_file,
                                                     output_format=OutputFormat.WIDE)
            records = converter.read_wpilog()
            print(f"Total amount of metrics pulled from the log: {len(converter.metrics_names)}")
            converter.convert(records)
            print(f"Successfully converted {converter.wpilog_file} to {converter.output_file}")
        case FileFormat.JSON:
            raise NotImplemented()
        case _:
            print("Please select and accepted format, Parquet, Avro, or Json")


if __name__ == "__main__":

    input_dir: str = "/Users/agadd1/Documents/Adam/GitHub/wpilog-parser/2025txwac-logs/"
    for file in os.listdir(input_dir):
        if not file.endswith('.wpilog'):
            continue
        input_file: str = f"{input_dir}/{file}"
        output_file: str = f"/Users/agadd1/Documents/Adam/GitHub/wpilog-parser/test-data/output/2025txwaclogs/long/{file.split('.')[0]}.parquet"
        print(output_file)
        parse_file(
            input_file,
            FileFormat.PARQUET,
            out_file=output_file
        )
