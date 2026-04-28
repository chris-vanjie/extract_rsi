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
import re
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)

RAD_TO_DEG = 57.295779505601  # 180/π — GPS lat/lon stored in radians in RSI binary

# Default layout — 512-channel RSI system (survey 9900010).
# These are used as fallback when no RSI_import.I2 sidecar is present.
_RECORD_SIZE_DEFAULT   = 2191
_N_CHANNELS_DEFAULT    = 512
_DOWN_SPEC_OFFSET_DEFAULT = 130
_UP_SPEC_OFFSET_DEFAULT   = 1167

# Keep top-level name for backward-compat imports
RECORD_SIZE = _RECORD_SIZE_DEFAULT


@dataclass
class I2Schema:
    """Key layout parameters extracted from an RSI_import.I2 sidecar."""
    record_size:       int = _RECORD_SIZE_DEFAULT
    n_channels:        int = _N_CHANNELS_DEFAULT
    down_spec_offset:  int = _DOWN_SPEC_OFFSET_DEFAULT
    up_spec_offset:    int = _UP_SPEC_OFFSET_DEFAULT


# Scalar field offsets, formats, and sizes — fixed across all RSI systems
# (only the spectrum section at the end of the record changes with channel count).
_SCALAR_NAMES   = [
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
]
_SCALAR_FORMATS = [
    "<u4",
    "<i4",
    "<i4", "<i4", "<i4", "<i4",
    "<i4",
    "<u2", "<u2", "u1",
    "<f4", "<f4",
    "u1",
    "<f4", "<f4", "<f4",
    "u1",
    "<u4", "<u4",
    "u1",
    "<u4",
]
_SCALAR_OFFSETS = [
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
]


def _make_header_dtype(record_size: int) -> np.dtype:
    """Build the scalar-field structured dtype with the correct itemsize."""
    return np.dtype({
        "names":    _SCALAR_NAMES,
        "formats":  _SCALAR_FORMATS,
        "offsets":  _SCALAR_OFFSETS,
        "itemsize": record_size,
    })


# Pre-built default dtype (512-channel system)
_HEADER_DTYPE = _make_header_dtype(_RECORD_SIZE_DEFAULT)


def parse_i2(path: Path) -> I2Schema:
    """Parse an RSI_import.I2 sidecar and return the key layout parameters.

    Extracts RECORDSIZE, DOWN_SPECTRUM channel count and offset, and
    UP_SPECTRUM offset.  All other fields fall back to defaults so the
    function is resilient to incomplete or variant I2 files.
    """
    schema = I2Schema()
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError as exc:
        log.warning("Could not read I2 sidecar %s: %s", path.name, exc)
        return schema

    lines = text.splitlines()
    i = 0
    while i < len(lines):
        line = lines[i].strip()

        # Keyword = value  (e.g. "RECORDSIZE      2191")
        upper = line.upper()
        if upper.startswith("RECORDSIZE") or upper.startswith("BLOCKSIZE"):
            parts = line.split()
            if len(parts) >= 2:
                try:
                    val = int(parts[1])
                    if upper.startswith("RECORDSIZE"):
                        schema.record_size = val
                except ValueError:
                    pass

        # DATA / CHAN pair — only care about array channels (spectra)
        if upper.startswith("DATA") and i + 1 < len(lines):
            data_rest = line[4:].strip()
            data_parts = [p.strip() for p in data_rest.split(",")]
            chan_line = lines[i + 1].strip()
            if chan_line.upper().startswith("CHAN") and len(data_parts) >= 1:
                chan_name_raw = chan_line[4:].strip().split(",")[0].strip()
                arr_m = re.match(r'^(\w+)\{(\d+)\}', chan_name_raw)
                if arr_m:
                    name  = arr_m.group(1).upper()
                    count = int(arr_m.group(2))
                    try:
                        offset = int(data_parts[0])
                    except ValueError:
                        offset = None
                    if name == "DOWN_SPECTRUM" and offset is not None:
                        schema.down_spec_offset = offset
                        schema.n_channels       = count
                    elif name == "UP_SPECTRUM" and offset is not None:
                        schema.up_spec_offset = offset

        i += 1

    log.info(
        "I2 schema from %s: record_size=%d  n_channels=%d  "
        "down_spec@%d  up_spec@%d",
        path.name, schema.record_size, schema.n_channels,
        schema.down_spec_offset, schema.up_spec_offset,
    )
    return schema


# Backward-compat aliases (default values)
_DOWN_SPEC_OFFSET = _DOWN_SPEC_OFFSET_DEFAULT
_UP_SPEC_OFFSET   = _UP_SPEC_OFFSET_DEFAULT
_N_CHANNELS       = _N_CHANNELS_DEFAULT


def read_rsibin(
    path: Path,
    i2_path: Path | None = None,
    include_spectra: bool = False,
) -> pd.DataFrame:
    """Read an RSI RadAssist BIN.rsibin file.

    Parameters
    ----------
    path            : path to BIN.rsibin
    i2_path         : path to RSI_import.I2 sidecar (auto-detected if None);
                      pass False to suppress sidecar lookup entirely
    include_spectra : if True, add down_spec_0..down_spec_{N-1} columns

    Returns
    -------
    Raw 2-Hz DataFrame. GPS lat/lon decoded to decimal degrees; invalid GPS
    positions (GPS_ERR != 0 or position is zero) replaced with NaN.
    """
    # Resolve sidecar: explicit path, auto-detect sibling, or use defaults
    schema = I2Schema()
    if i2_path is not False:
        candidate = i2_path if i2_path is not None else path.with_name("RSI_import.I2")
        if not candidate.exists():
            candidate = path.with_name("RSI_import.i2")
        if candidate.exists():
            schema = parse_i2(candidate)
        else:
            log.debug("No RSI_import.I2 sidecar found — using hardcoded defaults")

    record_size      = schema.record_size
    n_channels       = schema.n_channels
    down_spec_offset = schema.down_spec_offset
    up_spec_offset   = schema.up_spec_offset

    dtype = _make_header_dtype(record_size)

    log.info("Reading %s", path)
    raw_bytes = path.read_bytes()
    n = len(raw_bytes) // record_size
    remainder = len(raw_bytes) % record_size
    if remainder:
        log.warning("  File size %d not divisible by record_size %d — %d trailing bytes ignored",
                    len(raw_bytes), record_size, remainder)

    log.info("  %d records (%.2f hours at 2 Hz)", n, n / 2 / 3600)

    arr = np.frombuffer(raw_bytes[: n * record_size], dtype=dtype)

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
        buf = np.frombuffer(raw_bytes[: n * record_size], dtype=np.uint8).reshape(n, record_size)
        down = buf[:, down_spec_offset: down_spec_offset + n_channels * 2].view("<u2").reshape(n, n_channels)
        for ch in range(n_channels):
            df[f"down_spec_{ch}"] = down[:, ch].astype(np.int32)

    n_valid_gps = df["lat"].notna().sum()
    log.info("  %d records, %d with valid GPS (%.1f%%)",
             n, n_valid_gps, 100.0 * n_valid_gps / n if n else 0)

    return df.reset_index(drop=True)
