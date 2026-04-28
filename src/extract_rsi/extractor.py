"""extractor.py — Orchestrate RSI extraction into per-technology DataFrames.

Reads the raw BIN.rsibin (1 Hz or 2 Hz) and returns DataFrames at the
native acquisition rate.  In 2 Hz mode, duplicate integer timestamps are
resolved by offsetting the second record of each pair by +0.5 s.

  spec_df : SPEC DataFrame (radiometrics + GPS spine, at native Hz)
  nav_df  : NAV DataFrame  (GPS position + clearance, at native Hz)
"""
from __future__ import annotations

import logging
from datetime import date, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from .reader import read_rsibin
from .time_utils import leap_seconds_for_unix, unix_to_utc_1980, assign_2hz_offsets

log = logging.getLogger(__name__)

# Speed threshold for takeoff/landing detection (m/s ≈ 20 knots minimum)
_MIN_AIRSPEED_MS = 20.0
_EARTH_RADIUS_M  = 6_371_000.0


def _haversine_speed_1hz(lat: np.ndarray, lon: np.ndarray,
                          utc: np.ndarray) -> np.ndarray:
    """Ground speed (m/s) at 1 Hz from lat/lon/utc arrays."""
    dlat = np.deg2rad(np.diff(lat, prepend=lat[0]))
    dlon = np.deg2rad(np.diff(lon, prepend=lon[0]))
    lat_r = np.deg2rad(lat)
    a = (np.sin(dlat / 2) ** 2
         + np.cos(lat_r) * np.cos(np.roll(lat_r, 1)) * np.sin(dlon / 2) ** 2)
    dist = 2 * _EARTH_RADIUS_M * np.arcsin(np.sqrt(np.clip(a, 0, 1)))
    dt = np.abs(np.diff(utc, prepend=utc[0]))
    dt[dt == 0] = np.nan
    return dist / dt


def _find_takeoff_landing(nav_df: pd.DataFrame) -> tuple[float | None, float | None]:
    """Detect takeoff and landing from GPS speed.

    Returns (takeoff_utc_1980, landing_utc_1980) or (None, None).
    """
    lat = nav_df["lat"].to_numpy(dtype=float)
    lon = nav_df["lon"].to_numpy(dtype=float)
    utc = nav_df["utc_1980"].to_numpy(dtype=float)

    valid = ~(np.isnan(lat) | np.isnan(lon))
    if valid.sum() < 10:
        return None, None

    lat_ff = pd.Series(lat).ffill().bfill().to_numpy()
    lon_ff = pd.Series(lon).ffill().bfill().to_numpy()

    speed = _haversine_speed_1hz(lat_ff, lon_ff, utc)
    airborne = speed >= _MIN_AIRSPEED_MS

    if not airborne.any():
        return None, None

    first_idx = int(np.argmax(airborne))
    last_idx  = int(len(airborne) - 1 - np.argmax(airborne[::-1]))

    return float(utc[first_idx]), float(utc[last_idx])


def _build_spec_nav(raw: pd.DataFrame, leap: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Build SPEC and NAV DataFrames at native acquisition rate (1 Hz or 2 Hz).

    In 2 Hz mode the RSI records two records per integer second with the same
    timestamp.  assign_2hz_offsets() shifts the second record by +0.5 s so the
    time spine is evenly spaced.  No downsampling is applied — all records are
    preserved at their native rate.

    TC/K/U/Th counts are per-record (counts in 0.5 s at 2 Hz, counts in 1 s at
    1 Hz).  The _cps suffix reflects the standard SPEC contract column names.
    """
    df = raw.copy()

    # Fix timestamps: second record of each 2 Hz pair gets +0.5 s
    df["unix_time_adj"] = assign_2hz_offsets(df["unix_time"].to_numpy())
    df["utc_1980"] = unix_to_utc_1980(df["unix_time_adj"].to_numpy(), leap)

    spec = pd.DataFrame({
        "utc_1980":    df["utc_1980"],
        "lat":         df["lat"],
        "lon":         df["lon"],
        "alt_msl":     df["alt_msl"],
        "TC_cps":      df["tc"].astype(np.float64),
        "K_cps":       df["k"].astype(np.float64),
        "U_cps":       df["u"].astype(np.float64),
        "Th_cps":      df["th"].astype(np.float64),
        "gps_err":     df["gps_err"],
        "down_det":    df["down_det"],
        "down_live_s": df["down_live_s"],
        "fid":         df["record_no"],
    })

    # Forward-fill GPS positions over brief dropouts (GPS_ERR transients)
    for col in ["lat", "lon", "alt_msl"]:
        spec[col] = spec[col].ffill().bfill()

    # Pack spectrum per record (no summation)
    spec_cols = sorted(
        [c for c in df.columns if c.startswith("down_spec_")],
        key=lambda c: int(c.split("_")[-1]),
    )
    if spec_cols:
        n_channels = len(spec_cols)
        col_name = f"SPEC_spec{n_channels}down_raw"
        spec[col_name] = list(df[spec_cols].to_numpy().astype(np.float32))
        log.info("  Spectrum: %d channels per record → %s", n_channels, col_name)

    nav = pd.DataFrame({
        "utc_1980":  spec["utc_1980"],
        "lat":       spec["lat"],
        "lon":       spec["lon"],
        "alt_msl":   spec["alt_msl"],
        "clearance": np.nan,
        "fid":       spec["fid"],
    })

    dt_median = float(np.median(np.diff(spec["utc_1980"].to_numpy()[:200])))
    hz = round(1.0 / dt_median) if dt_median > 0 else 0
    log.info("  SPEC: %d records at ~%d Hz", len(spec), hz)
    log.info("  NAV:  %d records at ~%d Hz", len(nav), hz)

    return spec.reset_index(drop=True), nav.reset_index(drop=True)


def extract(raw_dir: Path, flight_id: str) -> dict:
    """Extract one RSI flight directory.

    Parameters
    ----------
    raw_dir   : directory containing BIN.rsibin (and RSI_import.I2)
    flight_id : e.g. "flt001"

    Returns
    -------
    dict with keys:
        spec_df      : pd.DataFrame  — 1 Hz SPEC (radiometrics + GPS)
        nav_df       : pd.DataFrame  — 1 Hz NAV
        raw_df       : pd.DataFrame  — raw 2 Hz parsed data
        flight_date  : date
        t_start      : float  — utc_1980 of first valid record
        t_end        : float  — utc_1980 of last valid record
        takeoff_gps  : float | None
        landing_gps  : float | None
        n_records    : int  — raw 2 Hz record count
        n_rows_1hz   : int  — 1 Hz row count
        gps_lock_pct : float
    """
    rsibin = raw_dir / "BIN.rsibin"
    if not rsibin.exists():
        raise FileNotFoundError(f"BIN.rsibin not found in {raw_dir}")

    i2_path = raw_dir / "RSI_import.I2"
    if not i2_path.exists():
        i2_path = raw_dir / "RSI_import.i2"

    raw = read_rsibin(rsibin, i2_path=i2_path if i2_path.exists() else None, include_spectra=True)

    # Drop zero-timestamp records (pre-acquisition startup)
    valid_mask = raw["unix_time"] > 0
    n_dropped = int((~valid_mask).sum())
    if n_dropped:
        log.info("  Dropped %d zero-timestamp records (pre-acquisition startup)", n_dropped)
    raw = raw[valid_mask].reset_index(drop=True)

    if raw.empty:
        raise ValueError(f"No valid records found in {rsibin}")

    # Determine leap-second count from first valid timestamp
    leap = leap_seconds_for_unix(float(raw["unix_time"].iloc[0]))
    log.info("  Leap seconds: %d", leap)

    flight_date = date.fromtimestamp(float(raw["unix_time"].iloc[0]))

    spec_df, nav_df = _build_spec_nav(raw, leap)

    takeoff_gps, landing_gps = _find_takeoff_landing(nav_df)
    if takeoff_gps:
        log.info("  Takeoff: utc_1980=%.1f  Landing: utc_1980=%.1f",
                 takeoff_gps, landing_gps or 0)
    else:
        log.warning("  Could not detect takeoff/landing from GPS speed")

    # Use gps_err (preserved from raw) to measure true GPS lock rate,
    # not the filled lat column (which is always non-null after ffill/bfill).
    gps_lock_pct = 100.0 * float((spec_df["gps_err"] == 0).mean())

    return {
        "spec_df":      spec_df,
        "nav_df":       nav_df,
        "raw_df":       raw,
        "flight_date":  flight_date,
        "t_start":      float(spec_df["utc_1980"].iloc[0]),
        "t_end":        float(spec_df["utc_1980"].iloc[-1]),
        "takeoff_gps":  takeoff_gps,
        "landing_gps":  landing_gps,
        "n_records":    len(raw),
        "n_rows_1hz":   len(spec_df),
        "gps_lock_pct": float(gps_lock_pct),
    }
