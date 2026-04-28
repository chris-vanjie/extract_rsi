# extract_rsi — Agent Instructions

At the end of each session, commit all changes with the session number and push to GitHub.

---

## Authoritative integration contract

Read **`/Volumes/T7/process_flight/INTEGRATION.md` §4a** before touching output
naming, report schema, or CLI flags. That document is the single source of truth
for what process_flight expects from this extractor.

Cross-pipeline standards (NAV naming, line list schema):
- `/Volumes/T7/process_flight/NAV_CONTRACT_STANDARDS.md`
- `/Volumes/T7/process_flight/EXTRACT_LINE_LIST_STANDARDS.md`

Bug history: `FIXES.md` in this repo.

Reference implementation: `/Volumes/T7/extract_xdas/` — the most complete
extract agent in the pipeline. Follow its patterns for CLI, writer, report,
and output naming unless the instrument format requires a different approach.

Note: RSI MDTpl template files exist in `/Volumes/T7/extract_xdas/legacy_code/MDTpl/`
(RSI_Spec_Down_Up.MDTpl, RSI_ROI.DW.UP.MDTpl, etc.) — these describe the RSI
radiometrics channel layout as recorded on an XDAS system. The standalone RSI
system may use a different format; compare carefully.

---

## Project overview

`extract_rsi` extracts raw RSI (Radiation Solutions Inc.) radiometric acquisition
system data into per-technology parquet DataFrames consumed by `process_flight`.

### Instrument summary

| Field | Value |
|-------|-------|
| Manufacturer | Radiation Solutions Inc. (RSI) |
| Raw file format | RSI RadAssist binary (`.rsibin`) — fixed-width 2191-byte records |
| Schema file | `RSI_import.I2` (in each flight dir) — describes byte offsets |
| Raw file extension | `.rsibin` |
| Technologies | Radiometrics (K, U, Th, TC) + GPS |
| Sample rate | **2 Hz** (two records per integer UTC second, ~0.5 s acquisition each) |
| Output rate | **2 Hz native** — no downsampling. Second record of each pair gets +0.5 s offset so time spine is evenly spaced. |
| GPS source | Embedded in binary: lon/lat (float32, **radians** ×57.295779505601=°) at offsets 96/100/104 |
| GPS time | Unix epoch (uint32, seconds since 1970-01-01) at offset 4 |
| Radar altimeter | **None fitted** — ADC_1/ADC_2 are discrete status inputs, not RALT |
| Dose rate | Not in raw file; excluded from SPEC output |

### Key binary field offsets (2191-byte records, no file header)

| Offset | Field | Type | Notes |
|--------|-------|------|-------|
| 4 | SMPL_UTC | uint32 | Unix seconds |
| 19 | GMM_DET_ERR | int32 | 0 = no error |
| 23 | RAW_TotCount | int32 | TC ROI counts |
| 27 | RAW_Potassium | int32 | K ROI counts |
| 31 | RAW_Uranium | int32 | U ROI counts |
| 35 | RAW_Thorium | int32 | Th ROI counts |
| 51 | RAW_UpDet | int32 | Up-detector ROI (all zeros if no UP detector) |
| 83 | GPS_ERR | uint8 | 0 = valid, 3 = no lock |
| 96 | LONGITUDE | float32 | Radians |
| 100 | LATITUDE | float32 | Radians |
| 104 | ALTITUDE | float32 | Metres MSL |
| 117 | DOWN_DET_COUNT | uint8 | Number of down detectors active |
| 118 | DOWN_ACQ_TIME | uint32 | Acquisition µs |
| 122 | DOWN_LIVE_TIME | uint32 | Live time µs |
| 130 | DOWN_SPECTRUM | 512×uint16 | 512-channel down spectrum (1024 bytes) |
| 1154 | UP_DET_COUNT | uint8 | |
| 1159 | UP_LIVE_TIME | uint32 | |
| 1167 | UP_SPECTRUM | 512×uint16 | 512-channel up spectrum (1024 bytes) |

Note: DOWN_COSMIC (offset 2176) and UP_COSMIC (offset 3213) in the I2 file are
erroneous (3213 > record size). These fields are ignored.

### GPS time conversion

```
utc_1980 = (unix_time - 315964800) + leap_seconds
# leap_seconds = 18 as of 2026-01-01
```

### Line detection note

The RSI binary has no embedded line number. Lines are detected from GPS track
using cross-track angle against the CTL bearing. Line numbers are assigned
sequentially from `FIRST FLIGHT LINE` in the CTL. For multi-flight surveys,
this produces the same sequence of line numbers in each flight — the integration
layer uses GPS coordinates to do final spatial line matching.

---

## Output contract (process_flight expects these)

All outputs written to `{out_dir}/` passed via `--out` CLI flag.

| File | Description |
|------|-------------|
| `{fid}_SPEC.parquet` | Radiometrics at native Hz (2 Hz for 9900010) + GPS spine + spectrum |
| `{fid}_NAV_RSI.parquet` | NAV at native Hz: GPS position + clearance |
| `{fid}_RSI_lines.csv` | Line list (see EXTRACT_LINE_LIST_STANDARDS.md) |
| `{fid}_RSI_report.json` | Extraction report — note: `_RSI_report.json` suffix (not `_report.json`) |

Additional outputs if the instrument carries them:
- `{fid}_ALT.parquet` — radar altimeter (if present in RSI data stream)
- `{fid}_metadata.json` — per-file hardware metadata

### SPEC parquet actual columns (RSI, confirmed against 9900010)

```
utc_1980              float64   GPS seconds since 1980-01-06 (process_flight adds gps_seconds alias)
lat                   float64   decimal degrees WGS-84 (forward-filled over GPS dropouts)
lon                   float64   decimal degrees WGS-84
alt_msl               float64   metres above mean sea level
TC_cps                float64   total count ROI — raw counts per 0.5 s record at 2 Hz
K_cps                 float64   potassium ROI counts per record
U_cps                 float64   uranium ROI counts per record
Th_cps                float64   thorium ROI counts per record
gps_err               uint8     0 = valid GPS, non-zero = error/no lock
down_det              uint8     number of downward detector crystals active
down_live_s           float64   live time per record (seconds, ~0.499 at 2 Hz)
fid                   int64     record_no from raw file (FID equivalent)
SPEC_spec512down_raw  object    512-element float32 array per row (9900010 system)
```

**Column name normalisation in qc_radiometrics:** `TC_cps → SPEC_tc_raw`, `K_cps → SPEC_k_raw`,
`U_cps → SPEC_u_raw`, `Th_cps → SPEC_th_raw`. Done automatically on load — no pipeline change needed.

**Dead time:** `spec_dead_frac = 1 - (down_live_s / sample_period)` where `sample_period = 1/hz`.
qc_radiometrics detects hz from the time column and normalises correctly for both 1 Hz and 2 Hz.

### NAV parquet required columns

```
gps_seconds   float64   GPS seconds since 1980-01-06 (leap-corrected)
lat           float64   decimal degrees WGS-84
lon           float64   decimal degrees WGS-84
alt_msl       float64   metres above mean sea level
clearance     float64   AGL metres (radar alt preferred, SRTM fallback, NaN if unknown)
```

### Lines CSV required columns

See `/Volumes/T7/process_flight/EXTRACT_LINE_LIST_STANDARDS.md` for the full schema.
Minimum: `flight_number, line_no, fid_start, fid_end, gps_start, gps_end`

---

## CLI interface

```bash
UV_PROJECT_ENVIRONMENT=/Users/chris/.venvs/extract_rsi UV_LINK_MODE=copy \
  uv run --project /Volumes/T7/extract_rsi extract-rsi run \
    --raw   /path/to/raw/flight/dir \
    --out   /path/to/processed/fltXXXX \
    --flight-id fltXXXX
```

process_flight calls this via subprocess — keep the interface stable.

---

## Module map

| Module | Purpose |
|--------|---------|
| `cli.py` | Click CLI entry point; CTL auto-discovery |
| `time_utils.py` | Unix→GPS time conversion, leap-second table, `assign_2hz_offsets()` |
| `reader.py` | Parse BIN.rsibin via numpy structured dtype; `parse_i2()` for I2 sidecar |
| `extractor.py` | Read → 2 Hz timestamp fix → SPEC + NAV DataFrames (native rate, no downsample) |
| `line_detect.py` | GPS track → survey line detection (cross-track angle, 3-pass merge) |
| `writer.py` | Write SPEC/NAV parquets, RSI_lines.csv, NAV sidecar JSON |
| `report.py` | Build `{fid}_RSI_report.json` |

---

## Venv

```
UV_PROJECT_ENVIRONMENT=/Users/chris/.venvs/extract_rsi
UV_LINK_MODE=copy
```

T7 is exFAT — venv lives on the local macOS filesystem, not on T7.

---

## BUGS.md / FIXES.md standard

- `BUGS.md` — active bugs and workarounds
- `FIXES.md` — closed bugs with resolution notes
- Follow the same format as `/Volumes/T7/extract_xdas/FIXES.md`
