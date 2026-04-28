"""report.py — Build {fid}_report.json for process_flight.

Schema follows INTEGRATION.md §4a (extract_xdas/extract_xagmag pattern):
  timing.takeoff_gps, landing_gps, file_start_gps, file_end_gps, ...
  qc_summary.verdict
"""
from __future__ import annotations

import json
import logging
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)


def build_report(
    result:      dict,
    lines_df:    pd.DataFrame,
    fid:         str,
    out_dir:     Path,
    flight_date: date,
) -> Path:
    """Build and write {fid}_report.json.

    Parameters
    ----------
    result      : dict returned by extractor.extract()
    lines_df    : line list DataFrame
    fid         : flight identifier
    out_dir     : output directory
    flight_date : observation date
    """
    spec_df = result["spec_df"]
    nav_df  = result["nav_df"]

    takeoff_gps = result.get("takeoff_gps")
    landing_gps = result.get("landing_gps")

    n_survey   = int((lines_df["line_no"] > 0).sum())
    n_segments = len(lines_df)

    # Radiometric statistics
    spec_summary: dict = {}
    for ch in ["TC_cps", "K_cps", "U_cps", "Th_cps"]:
        if ch in spec_df.columns:
            s = spec_df[ch].dropna()
            if not s.empty:
                spec_summary[ch] = {
                    "min":  round(float(s.min()),  2),
                    "max":  round(float(s.max()),  2),
                    "mean": round(float(s.mean()), 2),
                    "p95":  round(float(np.percentile(s, 95)), 2),
                }

    # GPS quality — use gps_err column (0 = locked)
    spec_df = result["spec_df"]
    n_valid_gps = int((spec_df["gps_err"] == 0).sum())
    gps_lock_pct = result["gps_lock_pct"]

    # QC verdict
    if gps_lock_pct < 50.0:
        verdict = "FAIL"
    elif gps_lock_pct < 90.0:
        verdict = "WARN"
    else:
        verdict = "PASS"

    report = {
        "pipeline":    "extract_rsi",
        "flight_id":   fid,
        "flight_date": flight_date.isoformat(),

        "timing": {
            "file_start_gps":   result["t_start"],
            "file_end_gps":     result["t_end"],
            "takeoff_gps":      takeoff_gps,
            "landing_gps":      landing_gps,
            "flight_start_gps": takeoff_gps,
            "flight_end_gps":   landing_gps,
            "timezone":         "UTC",
        },

        "data_summary": {
            "n_records_2hz": result["n_records"],
            "n_rows_1hz":    result["n_rows_1hz"],
            "sample_rate_raw_hz": 2,
            "sample_rate_out_hz": 1,
            "gps_lock_pct":   round(gps_lock_pct, 2),
            "n_valid_gps_rows": n_valid_gps,
        },

        "line_summary": {
            "n_survey_lines": n_survey,
            "n_segments":     n_segments,
        },

        "spec_summary": spec_summary,

        "qc_summary": {
            "gps_lock_pct": round(gps_lock_pct, 2),
            "verdict":      verdict,
        },
    }

    # Use {fid}_RSI_report.json so it does not collide with {fid}_report.json
    # when XAGMAG is co-located in the same PROCESSED directory.
    path = out_dir / f"{fid}_RSI_report.json"
    path.write_text(json.dumps(report, indent=2, default=str))
    log.info("Report → %s", path.name)
    return path
