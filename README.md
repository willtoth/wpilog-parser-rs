# wpilog-parser

Parse .wpilog files from frc robots to better format for data analysis

You can now run this as a cli tool.

simply run the a command in the parser directory such as :

```
uv run main.py /Users/agadd1/Documents/Adam/GitHub/wpilog-parser/2025txwac-logs \
  --out-root /Users/agadd1/Documents/Adam/GitHub/wpilog-parser/test-data/output/2025txwaclogs/wide \
  --file-format parquet \
  --output-format wide
```