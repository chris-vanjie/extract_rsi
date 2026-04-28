"""
extract_rsi — RSI radiometric acquisition system extraction library.

Modules (to be built):
    reader      — Raw file reader / parser
    extractor   — Orchestrate extraction → per-technology DataFrames
    writer      — Write DataFrames → parquet files + report JSON
    report      — Build {fid}_report.json
    cli         — Command-line interface entry point
"""

__version__ = "0.1.0"
