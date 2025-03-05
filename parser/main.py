import logging
import argparse
import os
from enum import Enum, auto
from typing import Optional, Tuple

from format.models import OutputFormat
from format.formats.parquet import FormatParquet

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class FileAction(Enum):
    SKIP = auto()
    OVERWRITE = auto()

def prompt_for_overwrite(filename: str, remember_choice: Optional[FileAction] = None) -> Tuple[FileAction, bool]:
    if remember_choice is not None:
        return remember_choice, True

    response = input(f"\nFile {filename} already exists.\nDo you want to:\n[s]kip\n[o]verwrite\n[S]kip all\n[O]verwrite all\n> ")

    if response == 'o':
        return FileAction.OVERWRITE, False
    elif response == 'O':
        return FileAction.OVERWRITE, True
    else:  # Default to skip for any other input
        if response == 'S':
            return FileAction.SKIP, True
        return FileAction.SKIP, False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Convert all .wpilog files in a directory to .parquet files')
    parser.add_argument('input_dir', type=str, help='The directory of the .wpilog files to convert')
    args = parser.parse_args()

    input_dir: str = args.input_dir
    remember_choice: Optional[FileAction] = None

    for file in os.listdir(input_dir):
        if not file.endswith('.wpilog'):
            continue

        input_file: str = f"{input_dir}/{file}"
        output_file: str = f"{input_dir}/{file.split('.')[0]}.parquet"

        if os.path.exists(output_file):
            action, remember = prompt_for_overwrite(output_file, remember_choice)
            if remember:
                remember_choice = action
            if action == FileAction.SKIP:
                print(f"Skipping {file}")
                continue

        print(f"Converting {file} to {output_file}")
        converter: FormatParquet = FormatParquet(wpilog_file=input_file,
                                                     output_file=output_file,
                                                     output_format=OutputFormat.WIDE)
        records = converter.read_wpilog()
        print(f"Total amount of metrics pulled from the log: {len(converter.metrics_names)}")
        converter.convert(records)
        print(f"Successfully converted {converter.wpilog_file} to {converter.output_file}")

    print("Finished converting all .wpilog files to .parquet files")