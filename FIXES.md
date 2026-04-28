# extract_rsi — Fix Log

---

## S59 — Initial implementation (2026-04-28)

Full binary extractor implemented:
- `reader.py`: numpy structured dtype with explicit field offsets; lat/lon decoded from radians;
  GPS_ERR masking; sidecar auto-detection.
- `extractor.py`: drop zero-timestamp startup records; 2 Hz→1 Hz downsampling (superseded S60).
- `time_utils.py`: GPS epoch conversion + leap-second table.
- `writer.py`: SPEC/NAV parquets, RSI_lines.csv, NAV sidecar JSON.
- `report.py`: `{fid}_RSI_report.json` (suffix avoids collision with XAGMAG's `{fid}_report.json`).

---

## S60 — I2 sidecar parsing + 2 Hz native output + spectrum (2026-04-28)

### I2 sidecar parsing (`reader.py`)

`parse_i2(path)` reads `RSI_import.I2` for RECORDSIZE, DOWN/UP_SPECTRUM byte offset and channel
count. `_make_header_dtype(record_size)` builds the numpy structured dtype dynamically so the same
code handles both 256-channel and 512-channel RSI systems. Falls back to hardcoded defaults
(RECORDSIZE=2191, 512 ch, DOWN_SPEC@130, UP_SPEC@1167) when sidecar is absent or unreadable.
`read_rsibin()` now accepts `i2_path` parameter; auto-detects `RSI_import.I2` / `RSI_import.i2`
in the flight directory when not supplied.

### 2 Hz native output (`extractor.py`, `time_utils.py`)

`_build_spec_nav()` no longer downsamples. `assign_2hz_offsets()` detects consecutive records
with the same integer timestamp (the RSI 2 Hz pair) and shifts the second by +0.5 s, giving a
proper evenly-spaced 0.5 s time spine. All records preserved at native rate (~2 Hz for 9900010).

### 512-channel downward spectrum (`extractor.py`, `writer.py`)

`read_rsibin()` called with `include_spectra=True`. Spectrum packed per-record as a 512-element
float32 numpy array in column `SPEC_spec512down_raw`. `write_spec()` auto-detects any column
matching `spec*down*raw` pattern and appends it to the output parquet.

**Results (9900010, all 4 flights):** 2.00 Hz detected, gaps=PASS, dt=PASS, K/U/Th peaks all
fit=True. SPEC parquet shape: ~34–41k rows × 15 cols (varies by flight length).
