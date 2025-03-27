import logging
import os
import time
from pathlib import Path
from typing import List

import typer

from format.models import OutputFormat, FileFormat
from format.formats.parquet import FormatParquet

app = typer.Typer()
logger: logging.Logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def convert_one_file(
        input_file: str,
        output_dir: str,
        file_format: FileFormat,
        output_format: OutputFormat,
) -> None:
    logger.info(f"Transforming {input_file} → {output_dir}")
    start_time: float = time.perf_counter()

    match file_format:
        case FileFormat.PARQUET:
            converter: FormatParquet = FormatParquet(
                wpilog_file=input_file,
                output_directory=output_dir,
                output_format=output_format
            )

            t0 = time.perf_counter()
            logger.info("Reading schema (infer only)...")
            _ = converter.read_wpilog(infer_schema_only=True)
            logger.info(f"Schema inferred in {time.perf_counter() - t0:.2f} seconds")

            t1 = time.perf_counter()
            logger.info("Reading log data...")
            records = converter.read_wpilog()
            logger.info(f"Read {len(records)} records in {time.perf_counter() - t1:.2f} seconds")
            logger.info(f"Found {len(converter.metrics_names)} metrics")

            t2 = time.perf_counter()
            logger.info("Writing to parquet...")
            converter.convert(records)
            logger.info(f"Parquet write complete in {time.perf_counter() - t2:.2f} seconds")

        case FileFormat.AVRO | FileFormat.JSON:
            raise NotImplementedError(f"{file_format} not yet supported.")
        case _:
            raise typer.BadParameter(f"Unsupported format: {file_format}")

    total_time = time.perf_counter() - start_time
    logger.info(f"🏁 Finished processing {input_file} in {total_time:.2f} seconds")


@app.command()
def parse_dir(
        in_dir: str = typer.Argument(..., help="Directory containing .wpilog files"),
        out_root: str = typer.Option(..., help="Root output directory for converted files"),
        file_format: FileFormat = typer.Option(FileFormat.PARQUET),
        output_format: OutputFormat = typer.Option(OutputFormat.WIDE),
) -> None:
    """
Convert all `.wpilog` files in a directory into the desired output format.
    """
    in_path: Path = Path(in_dir)
    out_path: Path = Path(out_root)

    if not in_path.is_dir():
        raise typer.BadParameter(f"{in_dir} is not a valid directory")

    wpilog_files: List[str] = [f for f in os.listdir(in_path) if f.endswith(".wpilog")]
    if not wpilog_files:
        typer.echo("No .wpilog files found.")
        raise typer.Exit()

    logger.info(f"📁 Found {len(wpilog_files)} .wpilog files in {in_dir}")
    total_start = time.perf_counter()

    for file_name in wpilog_files:
        input_file: Path = in_path / file_name
        output_dir: Path = out_path / file_name.split(".")[0]
        os.makedirs(output_dir, exist_ok=True)
        convert_one_file(str(input_file), str(output_dir), file_format, output_format)

    total_duration = time.perf_counter() - total_start
    logger.info(f"All files processed in {total_duration:.2f} seconds")


if __name__ == "__main__":
    app()

#
# if __name__ == "__main__":
#
#     input_dir: str = "/Users/agadd1/Documents/Adam/GitHub/wpilog-parser/2025txwac-logs/"
#     for file in os.listdir(input_dir):
#         if not file.endswith('.wpilog'):
#             continue
#         input_file: str = f"{input_dir}/{file}"
#         output_dir: str = f"/Users/agadd1/Documents/Adam/GitHub/wpilog-parser/test-data/output/2025txwaclogs/wide/{file.split('.')[0]}"
#         print(output_dir)
#         parse_file(
#             input_file,
#             FileFormat.PARQUET,
#             out_dir=output_dir
#         )
#
#     # typer.run(parse_file)
