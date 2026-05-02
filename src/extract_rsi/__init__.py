"""extract_rsi — RSI radiometric acquisition system extraction tool."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Optional

__version__ = "0.1.0"
__all__ = ["run", "extract"]


def extract(
    input_files,
    output_dir,
    flight: str,
    techs=None,
    keep_interim: bool = False,
    dry_run: bool = False,
) -> dict:
    """Extract a set of RSI BIN.rsibin files with continuity checking.

    See acq_extract.extract for full documentation.
    """
    from .acq_extract import extract as _extract
    return _extract(input_files, output_dir, flight, techs=techs,
                    keep_interim=keep_interim, dry_run=dry_run)


def run(raw_dir: Path, out_dir: Path, flight_id: str, plan: Optional[Path] = None) -> None:
    """Extract one RSI raw flight directory to parquet + report.

    Mirrors the ``extract-rsi run`` CLI command.

    Args:
        raw_dir:   Raw flight directory containing BIN.rsibin and RSI_import.I2.
        out_dir:   Output directory for parquet files and report JSON.
        flight_id: Flight identifier (e.g. "flt001").
        plan:      Optional CTL flight plan file. Auto-discovered if None.
    """
    from .extractor import extract
    from .line_detect import detect_lines
    from .writer import write_all
    from .report import build_report

    raw_dir = Path(raw_dir)
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Auto-discover CTL plan if not given (walk up from raw_dir)
    ctl_path: Optional[Path] = plan
    if ctl_path is None:
        candidate = raw_dir
        for _ in range(4):
            fp = candidate / "Flightplan"
            if fp.is_dir():
                ctls = sorted(fp.glob("*.ctl"))
                if ctls:
                    ctl_path = ctls[0]
                    break
            ctls = sorted(candidate.glob("*.ctl"))
            if ctls:
                ctl_path = ctls[0]
                break
            candidate = candidate.parent

    m = re.search(r"(\d+)$", flight_id)
    flight_num = int(m.group(1)) if m else 0

    result = extract(raw_dir, flight_id)
    lines_df = detect_lines(result["nav_df"], flight_num, ctl_path)
    write_all(result["spec_df"], result["nav_df"], lines_df, out_dir, flight_id)
    build_report(result, lines_df, flight_id, out_dir, result["flight_date"])
