"""line_detect.py — Detect survey lines from GPS track for RSI data.

The RSI binary contains no embedded line number.  Lines are inferred from:
  1. Speed threshold: aircraft must be moving to be on a survey line.
  2. Heading stability: survey lines are straight (low cross-track angle).
  3. Three-pass merge algorithm (EXTRACT_LINE_LIST_STANDARDS.md §3.5).

Line numbers are assigned from the .ctl flight plan geometry when available.
Without a .ctl a sequential counter is used and the caller is warned.
"""
from __future__ import annotations

import logging
import re
from pathlib import Path

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)

# Thresholds — validated defaults; tune per survey if needed
MIN_SPEED_MS        = 20.0   # m/s (~39 knots); below = ground or hovering
MAX_CROSS_TRACK_DEG = 25.0   # degrees from line bearing → on survey
MAX_TURN_RATE_DEG_S = 2.0    # fallback (no CTL): >2 deg/s = turning
STUB_THRESHOLD_S    = 30.0   # survey segment <30 s flanked by offline = stub
WOBBLE_THRESHOLD_S  = 30.0   # offline <30 s between same-line survey = wobble
EARTH_RADIUS_M      = 6_371_000.0


# ── geometry helpers ──────────────────────────────────────────────────────────

def _haversine_speed(lat: np.ndarray, lon: np.ndarray,
                     utc: np.ndarray) -> np.ndarray:
    dlat = np.deg2rad(np.diff(lat, prepend=lat[0]))
    dlon = np.deg2rad(np.diff(lon, prepend=lon[0]))
    lat_r = np.deg2rad(lat)
    a = (np.sin(dlat / 2) ** 2
         + np.cos(lat_r) * np.cos(np.roll(lat_r, 1)) * np.sin(dlon / 2) ** 2)
    dist = 2 * EARTH_RADIUS_M * np.arcsin(np.sqrt(np.clip(a, 0, 1)))
    dt = np.abs(np.diff(utc, prepend=utc[0]))
    dt[dt == 0] = np.nan
    return dist / dt


def _bearing(lat: np.ndarray, lon: np.ndarray) -> np.ndarray:
    lat1 = np.deg2rad(lat)
    lat2 = np.deg2rad(np.roll(lat, -1))
    dlon = np.deg2rad(np.roll(lon, -1) - lon)
    x = np.sin(dlon) * np.cos(lat2)
    y = (np.cos(lat1) * np.sin(lat2)
         - np.sin(lat1) * np.cos(lat2) * np.cos(dlon))
    brng = np.degrees(np.arctan2(x, y)) % 360.0
    brng[-1] = brng[-2]
    return brng


def _cross_track_angle(bearing: np.ndarray, line_bearing_deg: float) -> np.ndarray:
    line_rad = np.deg2rad(line_bearing_deg)
    brng_rad = np.deg2rad(bearing)
    diff_fwd = np.abs(np.degrees(
        np.arctan2(np.sin(brng_rad - line_rad), np.cos(brng_rad - line_rad))
    ))
    line_rev = line_rad + np.pi
    diff_rev = np.abs(np.degrees(
        np.arctan2(np.sin(brng_rad - line_rev), np.cos(brng_rad - line_rev))
    ))
    return np.minimum(diff_fwd, diff_rev)


def _turn_rate(bearing: np.ndarray) -> np.ndarray:
    d = np.diff(bearing, prepend=bearing[0])
    d = ((d + 180) % 360) - 180
    return np.abs(d)


# ── CTL geometry ─────────────────────────────────────────────────────────────

def _load_ctl_lines(ctl_path: Path) -> dict | None:
    """Parse first_line, line_increment, and line_bearing from a .ctl file."""
    try:
        text = ctl_path.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return None

    def _kv(key: str) -> str | None:
        m = re.search(rf"^{re.escape(key)}\s+([\d.+-]+)", text,
                      re.MULTILINE | re.IGNORECASE)
        return m.group(1).strip() if m else None

    try:
        return {
            "first_line":     int(_kv("FIRST FLIGHT LINE") or "0"),
            "line_increment": int(_kv("FLIGHT LINE INCREMENT") or "40"),
            "first_tie":      int(_kv("FIRST TIE LINE") or "0"),
            "tie_increment":  int(_kv("TIE LINE INCREMENT") or "40"),
            "line_bearing":   float(_kv("LINE BEARING") or "0"),
        }
    except (TypeError, ValueError) as exc:
        log.warning("Could not parse line geometry from %s: %s", ctl_path, exc)
        return None


# ── three-pass merge ──────────────────────────────────────────────────────────

def _merge_stubs(segments: list[dict]) -> list[dict]:
    """Pass 1: absorb survey stubs <STUB_THRESHOLD_S flanked by offline."""
    changed = True
    while changed:
        changed = False
        out: list[dict] = []
        i = 0
        while i < len(segments):
            if (i > 0 and i < len(segments) - 1
                    and not segments[i - 1]["on_survey"]
                    and segments[i]["on_survey"]
                    and not segments[i + 1]["on_survey"]
                    and (segments[i]["gps_end"] - segments[i]["gps_start"]) < STUB_THRESHOLD_S):
                merged = {
                    "on_survey": False,
                    "fid_start": out[-1]["fid_start"],
                    "fid_end":   segments[i + 1]["fid_end"],
                    "gps_start": out[-1]["gps_start"],
                    "gps_end":   segments[i + 1]["gps_end"],
                }
                out[-1] = merged
                i += 2
                changed = True
                continue
            out.append(segments[i])
            i += 1
        segments = out
    return segments


def _merge_wobbles(segments: list[dict]) -> list[dict]:
    """Pass 2: absorb offline wobbles <WOBBLE_THRESHOLD_S between same-line surveys."""
    changed = True
    while changed:
        changed = False
        out: list[dict] = []
        i = 0
        while i < len(segments):
            if (i > 0 and i < len(segments) - 1
                    and segments[i - 1]["on_survey"]
                    and not segments[i]["on_survey"]
                    and segments[i + 1]["on_survey"]
                    and (segments[i]["gps_end"] - segments[i]["gps_start"]) < WOBBLE_THRESHOLD_S):
                merged = {
                    "on_survey": True,
                    "fid_start": out[-1]["fid_start"],
                    "fid_end":   segments[i + 1]["fid_end"],
                    "gps_start": out[-1]["gps_start"],
                    "gps_end":   segments[i + 1]["gps_end"],
                }
                out[-1] = merged
                i += 2
                changed = True
                continue
            out.append(segments[i])
            i += 1
        segments = out
    return segments


def _assign_line_numbers(segments: list[dict],
                          ctl: dict | None,
                          flight_num: int) -> list[dict]:
    """Assign line_no: positive for survey, flight-scoped negative for offline."""
    survey_counter  = 0
    offline_counter = 0
    result = []
    for seg in segments:
        if seg["on_survey"]:
            if ctl is not None:
                line_no = ctl["first_line"] + survey_counter * ctl["line_increment"]
            else:
                line_no = survey_counter + 1
            survey_counter += 1
        else:
            if offline_counter == 0:
                line_no = -(flight_num * 1000)
            else:
                line_no = -(flight_num * 1000 + offline_counter)
            offline_counter += 1
        result.append({**seg, "line_no": line_no})

    # Ferry-out sentinel
    if result and not result[-1]["on_survey"]:
        result[-1]["line_no"] = -(flight_num * 1000 + 999)

    return result


# ── public entry point ────────────────────────────────────────────────────────

def detect_lines(nav_1hz: pd.DataFrame,
                 flight_num: int,
                 ctl_path: Path | None = None) -> pd.DataFrame:
    """Detect survey lines from a 1 Hz NAV DataFrame.

    Parameters
    ----------
    nav_1hz    : 1 Hz NAV with columns utc_1980, lat, lon, fid
    flight_num : integer flight number for offline encoding
    ctl_path   : optional path to .ctl flight plan file

    Returns
    -------
    DataFrame: flight_number, line_no, fid_start, fid_end, gps_start, gps_end
    """
    ctl = _load_ctl_lines(ctl_path) if ctl_path else None
    if ctl:
        log.info("CTL geometry: first_line=%d inc=%d bearing=%.0f°",
                 ctl["first_line"], ctl["line_increment"], ctl["line_bearing"])
    else:
        log.warning("No CTL geometry — using sequential line numbering")

    lat  = nav_1hz["lat"].to_numpy(dtype=float)
    lon  = nav_1hz["lon"].to_numpy(dtype=float)
    utc  = nav_1hz["utc_1980"].to_numpy(dtype=float)
    fid  = nav_1hz["fid"].to_numpy(dtype=np.int64)

    valid = ~(np.isnan(lat) | np.isnan(lon))
    if not valid.any():
        log.warning("No valid GPS positions — producing ferry-only line list")
        return _ferry_only(flight_num, int(fid[0]), int(fid[-1]),
                           float(utc[0]), float(utc[-1]))

    lat_ff = pd.Series(lat).ffill().bfill().to_numpy()
    lon_ff = pd.Series(lon).ffill().bfill().to_numpy()

    speed   = _haversine_speed(lat_ff, lon_ff, utc)
    bearing = _bearing(lat_ff, lon_ff)

    if ctl is not None:
        cta = _cross_track_angle(bearing, ctl["line_bearing"])
        on_survey = (speed >= MIN_SPEED_MS) & (cta <= MAX_CROSS_TRACK_DEG)
        log.info("  Using cross-track angle (line bearing=%.0f°)", ctl["line_bearing"])
    else:
        turn_rt = _turn_rate(bearing)
        on_survey = (speed >= MIN_SPEED_MS) & (turn_rt <= MAX_TURN_RATE_DEG_S)
        log.info("  No CTL — using turn-rate threshold (%.1f deg/s)", MAX_TURN_RATE_DEG_S)

    # Build raw segments
    segments: list[dict] = []
    current_state = on_survey[0]
    seg_start_i = 0
    for i in range(1, len(on_survey)):
        if on_survey[i] != current_state or i == len(on_survey) - 1:
            end_i = i if on_survey[i] != current_state else i + 1
            end_i = min(end_i, len(fid) - 1)
            segments.append({
                "on_survey": bool(current_state),
                "fid_start": int(fid[seg_start_i]),
                "fid_end":   int(fid[end_i]),
                "gps_start": float(utc[seg_start_i]),
                "gps_end":   float(utc[end_i]),
            })
            current_state = on_survey[i]
            seg_start_i = i

    if not segments:
        return _ferry_only(flight_num, int(fid[0]), int(fid[-1]),
                           float(utc[0]), float(utc[-1]))

    segments = _merge_stubs(segments)
    segments = _merge_wobbles(segments)
    n_survey = sum(1 for s in segments if s["on_survey"])

    log.info("  Segments: %d after merge (%d survey lines)", len(segments), n_survey)

    if n_survey == 0:
        log.warning("No survey lines detected — producing ferry-only line list")
        return _ferry_only(flight_num, int(fid[0]), int(fid[-1]),
                           float(utc[0]), float(utc[-1]))

    segments = _assign_line_numbers(segments, ctl, flight_num)

    rows = [
        {
            "flight_number": flight_num,
            "line_no":       s["line_no"],
            "fid_start":     s["fid_start"],
            "fid_end":       s["fid_end"],
            "gps_start":     s["gps_start"],
            "gps_end":       s["gps_end"],
        }
        for s in segments
    ]
    result = pd.DataFrame(rows)
    log.info("Lines → %d survey line(s), %d segment(s) total", n_survey, len(result))
    return result


def _ferry_only(flight_num: int, fid_start: int, fid_end: int,
                gps_start: float, gps_end: float) -> pd.DataFrame:
    log.warning("Lines → no survey lines flown — ferry/GPS only")
    return pd.DataFrame([{
        "flight_number": flight_num,
        "line_no":       -(flight_num * 1000),
        "fid_start":     fid_start,
        "fid_end":       fid_end,
        "gps_start":     gps_start,
        "gps_end":       gps_end,
    }])
