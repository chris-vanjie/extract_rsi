"""extractor.py — Orchestrate RSI extraction into per-technology DataFrames.

Reads the raw 2-Hz BIN.rsibin, downsamples to 1 Hz, and returns:
  spec_df : 1 Hz SPEC DataFrame (radiometrics + GPS spine)
  nav_df  : 1 Hz NAV DataFrame  (GPS position + clearance)
"""
from __future__ import annotations

import logging
from datetime import date, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from .reader import read_rsibin
from .time_utils import leap_seconds_for_unix, unix_to_utc_1980

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
    """Downsample 2 Hz raw records to 1 Hz SPEC and NAV DataFrames.

    Grouping: floor(unix_time) — integer UTC second.
    Counts are summed; GPS is averaged (NaN-safe).
    """
    raw = raw.copy()
    raw["utc_sec"] = np.floor(raw["unix_time"]).astype(np.int64)

    spec = (
        raw.groupby("utc_sec", sort=True)
        .agg(
            unix_time=("unix_time", "first"),
            lat=("lat", "mean"),
            lon=("lon", "mean"),
            alt_msl=("alt_msl", "mean"),
            TC_cps=("tc", "sum"),
            K_cps=("k", "sum"),
            U_cps=("u", "sum"),
            Th_cps=("th", "sum"),
            gps_err=("gps_err", "min"),       # 0 if any record in second had valid GPS
            down_det=("down_det", "max"),      # detector crystal count
            down_live_s=("down_live_s", "sum"), # total live time for this second
            fid=("record_no", "first"),        # first record number (FID equivalent)
        )
        .reset_index(drop=True)
    )

    spec["utc_1980"] = unix_to_utc_1980(spec["unix_time"].to_numpy(), leap)

    # Cast count columns to float64 per SPEC contract
    for col in ["TC_cps", "K_cps", "U_cps", "Th_cps"]:
        spec[col] = spec[col].astype(np.float64)

    # Forward-fill GPS positions over brief dropouts (GPS_ERR transients)
    for col in ["lat", "lon", "alt_msl"]:
        spec[col] = spec[col].ffill().bfill()

    # NAV: GPS position + clearance (no RALT fitted)
    nav = pd.DataFrame({
        "utc_1980": spec["utc_1980"],
        "lat":      spec["lat"],
        "lon":      spec["lon"],
        "alt_msl":  spec["alt_msl"],
        "clearance": np.nan,
        "fid":      spec["fid"],
    })

    log.info("  SPEC: %d rows at 1 Hz", len(spec))
    log.info("  NAV:  %d rows at 1 Hz", len(nav))

    return spec, nav


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

    raw = read_rsibin(rsibin, i2_path=i2_path if i2_path.exists() else None)

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
