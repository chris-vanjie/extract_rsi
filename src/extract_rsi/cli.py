"""CLI entry point for extract_rsi."""
from __future__ import annotations

import sys
from pathlib import Path

import click


@click.group()
def main() -> None:
    """extract-rsi — RSI radiometric acquisition system extractor."""


@main.command()
@click.option("--raw",       required=True, type=click.Path(exists=True, file_okay=False), help="Raw flight directory")
@click.option("--out",       required=True, type=click.Path(file_okay=False),              help="Output directory")
@click.option("--flight-id", required=True,                                                help="Flight ID (e.g. flt0002)")
@click.option("--verbose",   is_flag=True, default=False,                                  help="Verbose logging")
def run(raw: str, out: str, flight_id: str, verbose: bool) -> None:
    """Extract one raw RSI flight directory."""
    import logging
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=logging.DEBUG if verbose else logging.INFO,
        stream=sys.stdout,
    )
    log = logging.getLogger(__name__)

    raw_dir = Path(raw)
    out_dir = Path(out)
    out_dir.mkdir(parents=True, exist_ok=True)

    log.info("extract-rsi: raw=%s  out=%s  flight_id=%s", raw_dir, out_dir, flight_id)
    log.warning("NOT IMPLEMENTED — build reader.py, extractor.py, writer.py first")
    sys.exit(1)


if __name__ == "__main__":
    main()
