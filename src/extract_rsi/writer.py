"""writer.py — Write RSI extraction outputs.

Produces:
  {fid}_SPEC.parquet
  {fid}_NAV_RSI.parquet
  {fid}_NAV_RSI.json    (NAV contract sidecar, NAV_CONTRACT_STANDARDS.md)
  {fid}_RSI_lines.csv
"""
from __future__ import annotations

import json
import logging
from pathlib import Path

import pandas as pd

log = logging.getLogger(__name__)

NAV_CONTRACT_VERSION = "2"
PIPELINE_NAME        = "extract_rsi"

# SPEC columns in canonical order
_SPEC_COLS = [
    "utc_1980",
    "lat", "lon", "alt_msl",
    "TC_cps", "K_cps", "U_cps", "Th_cps",
    "gps_err", "down_det", "down_live_s", "fid",
]

# NAV columns required by contract
_NAV_REQUIRED = ["utc_1980", "lat", "lon", "alt_msl", "clearance"]


def write_spec(spec_df: pd.DataFrame, out_dir: Path, fid: str) -> Path:
    path = out_dir / f"{fid}_SPEC.parquet"
    # Write only the canonical columns that exist in spec_df
    cols = [c for c in _SPEC_COLS if c in spec_df.columns]
    spec_df[cols].to_parquet(path, index=False)
    log.info("SPEC → %s  (%d rows)", path.name, len(spec_df))
    return path


def write_nav(nav_df: pd.DataFrame, out_dir: Path, fid: str) -> Path:
    path = out_dir / f"{fid}_NAV_RSI.parquet"
    for col in _NAV_REQUIRED:
        if col not in nav_df.columns:
            raise ValueError(f"NAV DataFrame missing required column: {col}")
    nav_df.to_parquet(path, index=False)
    log.info("NAV  → %s  (%d rows)", path.name, len(nav_df))
    return path


def write_nav_sidecar(out_dir: Path, fid: str,
                       additional_files: dict[str, str]) -> Path:
    """Write the NAV contract sidecar JSON (NAV_CONTRACT_STANDARDS.md §3)."""
    sidecar = {
        "contract_version": NAV_CONTRACT_VERSION,
        "flight_id":        fid,
        "pipeline":         PIPELINE_NAME,
        "nav_file":         f"{fid}_NAV_RSI.parquet",

        "nav_contract": {
            "utc_1980":  "utc_1980",
            "lat":       "lat",
            "lon":       "lon",
            "alt_msl":   "alt_msl",
            "clearance": "clearance",
        },

        # No radar altimeter fitted on this instrument
        "clearance_available": False,
        "clearance_source":    None,

        "additional_files": additional_files,
    }
    path = out_dir / f"{fid}_NAV_RSI.json"
    path.write_text(json.dumps(sidecar, indent=2))
    log.info("NAV sidecar → %s", path.name)
    return path


def write_lines(lines_df: pd.DataFrame, out_dir: Path, fid: str) -> Path:
    path = out_dir / f"{fid}_RSI_lines.csv"
    cols = ["flight_number", "line_no", "fid_start", "fid_end", "gps_start", "gps_end"]
    lines_df[cols].to_csv(path, index=False)
    n_survey = int((lines_df["line_no"] > 0).sum())
    log.info("Lines → %s  (%d survey line(s), %d segment(s) total)",
             path.name, n_survey, len(lines_df))
    return path


def write_all(
    spec_df:  pd.DataFrame,
    nav_df:   pd.DataFrame,
    lines_df: pd.DataFrame,
    out_dir:  Path,
    fid:      str,
) -> dict[str, Path]:
    """Write all outputs; return {label: path}."""
    out_dir.mkdir(parents=True, exist_ok=True)

    spec_path  = write_spec(spec_df,  out_dir, fid)
    nav_path   = write_nav(nav_df,    out_dir, fid)
    lines_path = write_lines(lines_df, out_dir, fid)

    write_nav_sidecar(out_dir, fid, {"SPEC": spec_path.name})

    return {
        "SPEC":  spec_path,
        "NAV":   nav_path,
        "lines": lines_path,
    }
