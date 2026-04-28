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
| Raw file format | **UNKNOWN — fill in before first session** |
| Raw file extension | **UNKNOWN** |
| Technologies | Radiometrics (K, U, Th, TC, dose rate) + GPS |
| Sample rates | **UNKNOWN — typically 1 Hz** |
| GPS source | **UNKNOWN — embedded NMEA / separate receiver** |

> **First task for the implementing agent:** characterise one raw data file,
> document the format above, then proceed to implement.

---

## Output contract (process_flight expects these)

All outputs written to `{out_dir}/` passed via `--out` CLI flag.

| File | Description |
|------|-------------|
| `{fid}_SPEC.parquet` | Radiometrics at 1 Hz + GPS spine + lat/lon/alt_msl |
| `{fid}_NAV_RSI.parquet` | 1 Hz navigation: GPS position + derived clearance |
| `{fid}_RSI_lines.csv` | Line list (see EXTRACT_LINE_LIST_STANDARDS.md) |
| `{fid}_report.json` | Extraction report — schema in INTEGRATION.md §4a |

Additional outputs if the instrument carries them:
- `{fid}_ALT.parquet` — radar altimeter (if present in RSI data stream)
- `{fid}_metadata.json` — per-file hardware metadata

### SPEC parquet expected columns (RSI)

```
gps_seconds   float64   GPS seconds since 1980-01-06 (leap-corrected)
lat           float64   decimal degrees WGS-84
lon           float64   decimal degrees WGS-84
alt_msl       float64   metres above mean sea level
K_cps         float64   potassium window counts per second
U_cps         float64   uranium window counts per second
Th_cps        float64   thorium window counts per second
TC_cps        float64   total count counts per second
dose_nGy_h    float64   dose rate nGy/h (if available)
```

Column names are provisional — confirm against actual RSI output format.

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

## Module map (fill in as modules are built)

| Module | Purpose |
|--------|---------|
| `cli.py` | Click CLI entry point |
| `reader.py` | Raw file reader / parser — **TO BUILD** |
| `extractor.py` | Orchestrate extraction → per-technology DataFrames — **TO BUILD** |
| `writer.py` | Write parquets + report JSON — **TO BUILD** |
| `report.py` | Build `{fid}_report.json` — **TO BUILD** |

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
