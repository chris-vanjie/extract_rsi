"""CLI entry point for extract_rsi."""
from __future__ import annotations

import re
import sys
import logging
from pathlib import Path

import click


def _flight_num(flight_id: str) -> int:
    """Extract trailing integer from flight_id (e.g. 'flt001' → 1)."""
    m = re.search(r"(\d+)$", flight_id)
    return int(m.group(1)) if m else 0


def _find_ctl(raw_dir: Path) -> Path | None:
    """Walk up from raw_dir looking for a Flightplan/*.ctl or *.ctl file."""
    candidate = raw_dir
    for _ in range(4):
        # Check Flightplan/ subdirectory first
        fp = candidate / "Flightplan"
        if fp.is_dir():
            ctls = sorted(fp.glob("*.ctl"))
            if ctls:
                return ctls[0]
        # Also check current level
        ctls = sorted(candidate.glob("*.ctl"))
        if ctls:
            return ctls[0]
        candidate = candidate.parent
    return None


@click.group()
def main() -> None:
    """extract-rsi — RSI radiometric acquisition system extractor."""


@main.command()
@click.option("--raw",       required=True, type=click.Path(exists=True, file_okay=False),
              help="Raw flight directory (contains BIN.rsibin)")
@click.option("--out",       required=True, type=click.Path(file_okay=False),
              help="Output directory")
@click.option("--flight-id", required=True,
              help="Flight ID (e.g. flt001)")
@click.option("--plan",      default=None,  type=click.Path(exists=True, dir_okay=False),
              help="CTL flight plan file (auto-discovered if omitted)")
@click.option("--verbose",   is_flag=True,  default=False,
              help="Verbose logging")
def run(raw: str, out: str, flight_id: str,
        plan: str | None, verbose: bool) -> None:
    """Extract one raw RSI flight directory."""
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

    # Resolve CTL path
    ctl_path: Path | None = Path(plan) if plan else _find_ctl(raw_dir)
    if ctl_path:
        log.info("  CTL: %s", ctl_path)
    else:
        log.warning("  CTL: not found — line numbers will be sequential")

    from .extractor import extract
    from .line_detect import detect_lines
    from .writer import write_all
    from .report import build_report

    try:
        result = extract(raw_dir, flight_id)
    except (FileNotFoundError, ValueError) as exc:
        log.error("Extraction failed: %s", exc)
        sys.exit(1)

    flight_num = _flight_num(flight_id)
    lines_df = detect_lines(result["nav_df"], flight_num, ctl_path)

    write_all(result["spec_df"], result["nav_df"], lines_df, out_dir, flight_id)
    build_report(result, lines_df, flight_id, out_dir, result["flight_date"])

    log.info("extract-rsi: done — outputs in %s", out_dir)


if __name__ == "__main__":
    main()
