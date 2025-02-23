import logging

from format.models import OutputFormat
from format.formats.parquet import FormatParquet
import os

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

if __name__ == "__main__":

    input_dir: str = "/Users/agadd1/Documents/Adam/GitHub/wpilog-parser/test-data/input/2024txwaclogs"

    for file in os.listdir(input_dir):
        if not file.endswith('.wpilog'):
            continue
        input_file: str = f"{input_dir}/{file}"
        output_file: str = f"/Users/agadd1/Documents/Adam/GitHub/wpilog-parser/test-data/output/2024txwaclogs/wide/{file.split('.')[0]}.parquet"
        print(output_file)
        converter: FormatParquet = FormatParquet(wpilog_file=input_file,
                                                     output_file=output_file,
                                                     output_format=OutputFormat.WIDE)
        records = converter.read_wpilog()
        print(f"Total amount of metrics pulled from the log: {len(converter.metrics_names)}")
        converter.convert(records)
        print(f"Successfully converted {converter.wpilog_file} to {converter.output_file}")

        break
