"""reader.py — Parse RSI RadAssist binary file (BIN.rsibin).

Format is described by RSI_import.I2 in each flight directory.
Key parameters (all flights in survey 9900010):
  RECORDSIZE      2191 bytes
  FILEHEADER      0    (no header)
  RECORDSPERBLOCK 1

Field layout (byte offsets within each record):
  4   : SMPL_UTC   uint32  Unix seconds since 1970-01-01
  19  : GMM_DET_ERR int32  GMM detector error flags (0 = no error)
  23  : RAW_TotCount int32  Total-count ROI (channels 137–937)
  27  : RAW_Potassium int32  K ROI (channels 457–523)
  31  : RAW_Uranium   int32  U ROI (channels 553–620)
  35  : RAW_Thorium   int32  Th ROI (channels 803–937)
  51  : RAW_UpDet     int32  Up-detector ROI (ROI 8, channels 553–620)
  65  : NTR_DET_ERR   uint16 Neutron-detector error flags
  67  : NTR_TOTAL     uint16 Total neutron counts
  70  : NTR_TUBES     uint8  Number of neutron tubes summed
  75  : ADC_1         float32 Analog input 1 (discrete status input, NOT RALT)
  79  : ADC_2         float32 Analog input 2 (discrete status input, NOT RALT)
  83  : GPS_ERR       uint8  GPS error flag (0 = valid)
  96  : LONGITUDE     float32 Radians × 57.295779505601 → degrees E
  100 : LATITUDE      float32 Radians × 57.295779505601 → degrees N
  104 : ALTITUDE      float32 Metres above MSL
  117 : DOWN_DET_COUNT uint8  Number of downward detectors active
  118 : DOWN_ACQ_TIME  uint32 Acquisition time in microseconds
  122 : DOWN_LIVE_TIME uint32 Live time in microseconds
  130 : DOWN_SPECTRUM  512 × uint16  Downward spectrum (channels 0–511)
  1154: UP_DET_COUNT   uint8  Number of upward detectors active
  1159: UP_LIVE_TIME   uint32 Microseconds
  1167: UP_SPECTRUM    512 × uint16  Upward spectrum (0–511); all zeros if no UP detector

Note: DOWN_COSMIC (offset 2176) and UP_COSMIC (offset 3213) in the I2 file
appear to reference positions outside or at the edge of the record and are
ignored here.
"""
from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)

RECORD_SIZE = 2191
RAD_TO_DEG = 57.295779505601  # 180/π — GPS lat/lon stored in radians in RSI binary

# Numpy structured dtype with explicit byte offsets for efficient bulk extraction.
# The 'itemsize' key forces each element to span exactly one record.
_HEADER_DTYPE = np.dtype({
    "names": [
        "unix_time",
        "gdet_err",
        "tc", "k", "u", "th",
        "up_roi",
        "ntr_err", "ntr_total", "ntr_tubes",
        "adc1", "adc2",
        "gps_err",
        "lon_rad", "lat_rad", "alt_m",
        "down_det",
        "down_acq_us", "down_live_us",
        "up_det",
        "up_live_us",
    ],
    "formats": [
        "<u4",              # unix_time
        "<i4",              # gdet_err
        "<i4", "<i4", "<i4", "<i4",  # tc, k, u, th
        "<i4",              # up_roi
        "<u2", "<u2", "u1", # ntr_err, ntr_total, ntr_tubes
        "<f4", "<f4",       # adc1, adc2
        "u1",               # gps_err
        "<f4", "<f4", "<f4", # lon_rad, lat_rad, alt_m
        "u1",               # down_det
        "<u4", "<u4",       # down_acq_us, down_live_us
        "u1",               # up_det
        "<u4",              # up_live_us
    ],
    "offsets": [
        4,
        19,
        23, 27, 31, 35,
        51,
        65, 67, 70,
        75, 79,
        83,
        96, 100, 104,
        117,
        118, 122,
        1154,
        1159,
    ],
    "itemsize": RECORD_SIZE,
})

# Spectrum section offsets and channel count
_DOWN_SPEC_OFFSET = 130
_UP_SPEC_OFFSET   = 1167
_N_CHANNELS       = 512


def read_rsibin(path: Path, include_spectra: bool = False) -> pd.DataFrame:
    """Read an RSI RadAssist BIN.rsibin file.

    Parameters
    ----------
    path            : path to BIN.rsibin
    include_spectra : if True, add down_spec_0..down_spec_511 columns
                      (adds ~2 MB per 10k records; omit for routine extraction)

    Returns
    -------
    Raw 2-Hz DataFrame. GPS lat/lon decoded to decimal degrees; invalid GPS
    positions (GPS_ERR != 0 or position is zero) replaced with NaN.
    """
    log.info("Reading %s", path)
    raw_bytes = path.read_bytes()
    n = len(raw_bytes) // RECORD_SIZE
    remainder = len(raw_bytes) % RECORD_SIZE
    if remainder:
        log.warning("  File size %d not divisible by RECORD_SIZE %d — %d trailing bytes ignored",
                    len(raw_bytes), RECORD_SIZE, remainder)

    log.info("  %d records (%.2f hours at 2 Hz)", n, n / 2 / 3600)

    arr = np.frombuffer(raw_bytes[: n * RECORD_SIZE], dtype=_HEADER_DTYPE)

    df = pd.DataFrame({
        "unix_time":    arr["unix_time"].astype(np.float64),
        "gdet_err":     arr["gdet_err"].astype(np.int32),
        "tc":           arr["tc"].astype(np.int32),
        "k":            arr["k"].astype(np.int32),
        "u":            arr["u"].astype(np.int32),
        "th":           arr["th"].astype(np.int32),
        "up_roi":       arr["up_roi"].astype(np.int32),
        "ntr_err":      arr["ntr_err"].astype(np.int16),
        "ntr_total":    arr["ntr_total"].astype(np.int16),
        "ntr_tubes":    arr["ntr_tubes"].astype(np.uint8),
        "adc1":         arr["adc1"].astype(np.float32),
        "adc2":         arr["adc2"].astype(np.float32),
        "gps_err":      arr["gps_err"].astype(np.uint8),
        "lon":          arr["lon_rad"].astype(np.float64) * RAD_TO_DEG,
        "lat":          arr["lat_rad"].astype(np.float64) * RAD_TO_DEG,
        "alt_msl":      arr["alt_m"].astype(np.float64),
        "down_det":     arr["down_det"].astype(np.uint8),
        "down_acq_s":   arr["down_acq_us"].astype(np.float64) * 1e-6,
        "down_live_s":  arr["down_live_us"].astype(np.float64) * 1e-6,
        "up_det":       arr["up_det"].astype(np.uint8),
        "up_live_s":    arr["up_live_us"].astype(np.float64) * 1e-6,
        "record_no":    np.arange(n, dtype=np.int64),
    })

    # Mask invalid GPS: GPS_ERR != 0 OR position is zero (no lock yet)
    invalid_gps = (df["gps_err"] != 0) | (
        (df["lat"].abs() < 1e-6) & (df["lon"].abs() < 1e-6)
    )
    df.loc[invalid_gps, ["lat", "lon", "alt_msl"]] = np.nan

    if include_spectra:
        buf = np.frombuffer(raw_bytes[: n * RECORD_SIZE], dtype=np.uint8).reshape(n, RECORD_SIZE)
        down = buf[:, _DOWN_SPEC_OFFSET: _DOWN_SPEC_OFFSET + _N_CHANNELS * 2].view("<u2").reshape(n, _N_CHANNELS)
        for ch in range(_N_CHANNELS):
            df[f"down_spec_{ch}"] = down[:, ch].astype(np.int32)

    n_valid_gps = df["lat"].notna().sum()
    log.info("  %d records, %d with valid GPS (%.1f%%)",
             n, n_valid_gps, 100.0 * n_valid_gps / n if n else 0)

    return df.reset_index(drop=True)
