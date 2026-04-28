"""time_utils.py — GPS time conversion for extract_rsi."""
from __future__ import annotations

from datetime import date

import numpy as np

# Unix timestamp of the GPS epoch (1980-01-06 00:00:00 UTC)
GPS_EPOCH_UNIX: int = 315964800

# Leap-second table: (date_from, leap_count).
# GPS clock is ahead of UTC by leap_count seconds from date_from onwards.
# Current value: 18 s (since 2017-01-01). Update if new leap seconds are announced.
_LEAP_SECONDS: list[tuple[date, int]] = [
    (date(2017, 1, 1), 18),
    (date(2015, 7, 1), 17),
    (date(2012, 7, 1), 16),
    (date(2009, 1, 1), 15),
    (date(2006, 1, 1), 14),
    (date(1999, 1, 1), 13),
    (date(1997, 7, 1), 12),
    (date(1996, 1, 1), 11),
    (date(1994, 7, 1), 10),
    (date(1993, 7, 1),  9),
    (date(1992, 7, 1),  8),
    (date(1991, 1, 1),  7),
    (date(1990, 1, 1),  6),
    (date(1988, 1, 1),  5),
    (date(1985, 7, 1),  4),
    (date(1983, 7, 1),  3),
    (date(1982, 7, 1),  2),
    (date(1981, 7, 1),  1),
    (date(1980, 1, 6),  0),
]


def leap_seconds_for_date(obs_date: date) -> int:
    """Return leap-second count applicable on obs_date."""
    for cutoff, count in _LEAP_SECONDS:
        if obs_date >= cutoff:
            return count
    return 0


def leap_seconds_for_unix(unix_ts: float) -> int:
    """Return leap-second count for a given Unix timestamp."""
    return leap_seconds_for_date(date.fromtimestamp(unix_ts))


def unix_to_utc_1980(unix_arr: np.ndarray, leap_seconds: int) -> np.ndarray:
    """Convert an array of Unix timestamps to utc_1980 (GPS seconds).

    utc_1980 = (unix_time - GPS_EPOCH_UNIX) + leap_seconds

    The leap_seconds term corrects for GPS time running ahead of UTC.
    For 2026 data: leap_seconds = 18.
    """
    return (unix_arr.astype(np.float64) - GPS_EPOCH_UNIX) + leap_seconds


def assign_2hz_offsets(unix_time: np.ndarray) -> np.ndarray:
    """Offset the second record of each 2 Hz pair by +0.5 s.

    The RSI records only an integer-second timestamp.  In 2 Hz mode both
    records in a pair share the same integer value.  This function detects
    such pairs and shifts the later record by +0.5 s so the output time
    spine is evenly spaced at 0.5 s intervals.  Records that already have
    a unique integer timestamp (1 Hz mode) are left unchanged.
    """
    t = unix_time.astype(np.float64).copy()
    int_t = np.floor(t).astype(np.int64)
    # Mark every record whose integer timestamp equals the preceding record's
    same_as_prev = np.zeros(len(t), dtype=bool)
    same_as_prev[1:] = int_t[1:] == int_t[:-1]
    t[same_as_prev] += 0.5
    return t
