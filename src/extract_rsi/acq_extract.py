"""
acq_extract.py — Multi-file extraction with continuity checking for RSI.

Processes a list of RSI BIN.rsibin files, groups them by time continuity
(gap ≤ MAX_GAP_S = 1800 s / 30 min), and merges each continuous group
into a single set of output files.

Output stems:
    Single continuous set  : {flight}
    Multiple sets          : {flight}_set01, {flight}_set02, …
"""
from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Optional, Sequence

import pandas as pd

log = logging.getLogger(__name__)

MAX_GAP_S = 1800.0  # 30 minutes


# ---------------------------------------------------------------------------
# Continuity helpers
# ---------------------------------------------------------------------------

def _group_continuous(file_bounds: list[tuple[float, float]]) -> list[list[int]]:
    """Group file indices into continuous sets (gap ≤ MAX_GAP_S)."""
    if not file_bounds:
        return []
    order = sorted(range(len(file_bounds)), key=lambda i: file_bounds[i][0])
    groups: list[list[int]] = [[order[0]]]
    for idx in order[1:]:
        prev_end = file_bounds[groups[-1][-1]][1]
        curr_start = file_bounds[idx][0]
        gap = curr_start - prev_end
        if gap <= MAX_GAP_S:
            groups[-1].append(idx)
        else:
            log.warning(
                "RSI: %.1f s gap (%.1f min) between file %d and %d — new set",
                gap, gap / 60.0, groups[-1][-1], idx,
            )
            groups.append([idx])
    return groups


def _set_stem(flight: str, set_idx: int, n_sets: int) -> str:
    return flight if n_sets == 1 else f"{flight}_set{set_idx + 1:02d}"


def _find_ctl(search_root: Path) -> Optional[Path]:
    """Walk up from search_root looking for Flightplan/*.ctl."""
    candidate = search_root
    for _ in range(5):
        fp = candidate / "Flightplan"
        if fp.is_dir():
            ctls = sorted(fp.glob("*.ctl"))
            if ctls:
                return ctls[0]
        ctls = sorted(candidate.glob("*.ctl"))
        if ctls:
            return ctls[0]
        candidate = candidate.parent
    return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def extract(
    input_files: Sequence[Path],
    output_dir: Path,
    flight: str,
    techs: Optional[list[str]] = None,
    keep_interim: bool = False,
    dry_run: bool = False,
) -> dict:
    """Extract a set of RSI BIN.rsibin files with continuity checking.

    Parameters
    ----------
    input_files : list of BIN.rsibin file paths to process
    output_dir  : directory for final output files
    flight      : flight identifier stem (e.g. "flt0001")
    techs       : technologies to output (None = all).  NAV always written.
                  Supported: "SPEC".
    keep_interim: reserved — RSI extraction is in-memory, no interim files
    dry_run     : if True, extract in memory but write nothing

    Returns
    -------
    dict with keys:
        n_sets     : int
        sets       : list[dict]  — per-set metadata and output paths
        warnings   : list[str]
        continuous : bool
    """
    from .extractor import extract as _extract_file
    from .line_detect import detect_lines
    from .writer import write_all, write_nav, write_nav_sidecar

    input_files = [Path(f) for f in input_files]
    output_dir = Path(output_dir)

    if not input_files:
        return {"n_sets": 0, "sets": [], "warnings": ["No input files provided"], "continuous": True}

    write_spec = techs is None or "SPEC" in techs

    # ---- Step 1: per-file extraction (in-memory) ----------------------------
    file_results: list[dict] = []
    for i, rsibin in enumerate(input_files):
        log.info("RSI: extracting file %d/%d: %s", i + 1, len(input_files), rsibin.name)
        result = _extract_file(raw_dir=rsibin.parent, flight_id=f"file{i:02d}")
        file_results.append({
            "file": rsibin,
            "t_start": result["t_start"],
            "t_end": result["t_end"],
            "result": result,
        })
        log.info(
            "  t_start=%.1f  t_end=%.1f  records=%d  gps_lock=%.1f%%",
            result["t_start"], result["t_end"],
            result["n_records"], result["gps_lock_pct"],
        )

    # ---- Step 2: group by continuity ----------------------------------------
    bounds = [(r["t_start"], r["t_end"]) for r in file_results]
    groups = _group_continuous(bounds)
    n_sets = len(groups)

    warnings: list[str] = []
    if n_sets > 1:
        msg = (
            f"RSI: {len(input_files)} files form {n_sets} non-continuous sets "
            f"(gap > {MAX_GAP_S:.0f} s / {MAX_GAP_S / 60:.0f} min)"
        )
        log.warning(msg)
        warnings.append(msg)

    # ---- Step 3: merge and write per set ------------------------------------
    if not dry_run:
        output_dir.mkdir(parents=True, exist_ok=True)

    m = re.search(r"(\d+)$", flight)
    flight_num = int(m.group(1)) if m else 0

    sets_info: list[dict] = []
    for set_idx, group in enumerate(groups):
        stem = _set_stem(flight, set_idx, n_sets)

        spec_merged = (
            pd.concat([file_results[i]["result"]["spec_df"] for i in group], ignore_index=True)
            .sort_values("utc_1980").reset_index(drop=True)
        )
        nav_merged = (
            pd.concat([file_results[i]["result"]["nav_df"] for i in group], ignore_index=True)
            .sort_values("utc_1980").reset_index(drop=True)
        )

        # Auto-discover CTL flight plan from first file in this set
        first_file = file_results[group[0]]["file"]
        ctl_path = _find_ctl(first_file.parent)

        lines_df = detect_lines(nav_merged, flight_num=flight_num, ctl_path=ctl_path)

        written_paths: dict[str, Path] = {}
        if not dry_run:
            if write_spec:
                written_paths.update(write_all(spec_merged, nav_merged, lines_df, output_dir, stem))
            else:
                nav_path = write_nav(nav_merged, output_dir, stem)
                write_nav_sidecar(output_dir, stem, {})
                written_paths["NAV"] = nav_path
        else:
            log.info("[dry-run] RSI set '%s' → %s", stem, output_dir)

        sets_info.append({
            "stem": stem,
            "file_indices": group,
            "files": [file_results[i]["file"] for i in group],
            "t_start": file_results[group[0]]["t_start"],
            "t_end": file_results[group[-1]]["t_end"],
            "paths": written_paths,
        })
        log.info("RSI: set '%s' written (%d file(s))", stem, len(group))

    return {
        "n_sets": n_sets,
        "sets": sets_info,
        "warnings": warnings,
        "continuous": n_sets == 1,
    }
