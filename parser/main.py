import logging
import sys

from format.models import OutputFormat
from format.formats.parquet import FormatParquet


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

if __name__ == "__main__":

    input_file: str = "/Users/agadd1/Documents/Adam/GitHub/wpilog-parser/test-data/input/2024txwaclogs/FRC_20240301_214638_TXWAC_Q3.wpilog"

    output_file: str = "/Users/agadd1/Documents/Adam/GitHub/wpilog-parser/test-data/output/2024txwaclogs/wide/FRC_20240301_214638_TXWAC_Q3.parquet"

    converter: FormatParquet = FormatParquet(wpilog_file=input_file,
                                                 output_file=output_file,
                                                 output_format=OutputFormat.WIDE)
    records = converter.read_wpilog()
    converter.convert(records)
    print(f"Successfully converted {converter.wpilog_file} to {converter.output_file}")
