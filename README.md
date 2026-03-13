# OpenET and Vegetation Data Downloader

This repository downloads field-level time series from Climate Engine for agricultural polygons, then saves clean monthly summaries.

## Workflow

The script:

1. Reads a GeoJSON of field boundaries.
2. Calls the Climate Engine API for each field.
3. Pulls data from three datasets:
	 - LANDSAT_SR
	 - OPENET_CONUS
	 - SENTINEL2_SR
4. Aggregates values to monthly mean and monthly max per field.
5. Writes outputs as Parquet files for easy use in Python, R, or data platforms.

## Requirements

1. Python 3.10+.
2. A Climate Engine API key.

## One-time setup

From the project folder, run:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Set your API key in the same terminal:

```bash
export CLIMATE_ENGINE_API_KEY="YOUR_API_KEY_HERE"
```

Optional: place the key in a `.env` file in this folder:

```env
CLIMATE_ENGINE_API_KEY=YOUR_API_KEY_HERE
```

## Run the download

Standard run:

```bash
python ag_fields_timeseries_api.py --output-dir data/output --concurrency 10 --chunk-size 25
```

Small test run (first 3 fields only):

```bash
python ag_fields_timeseries_api.py --output-dir data/output --limit-fields 3
```

Optional pre-check of dataset metadata:

```bash
python ag_fields_timeseries_api.py --output-dir data/output --verify
```

## Where results are saved

Outputs go under `data/output` by default.

- Individual field files:
	- `data/output/individual/LANDSAT_SR/<field_id>.parquet`
	- `data/output/individual/OPENET_CONUS/<field_id>.parquet`
	- `data/output/individual/SENTINEL2_SR/<field_id>.parquet`
- Combined dataset files:
	- `data/output/combined/LANDSAT_SR-part-*.parquet`
	- `data/output/combined/OPENET_CONUS-part-*.parquet`
	- `data/output/combined/SENTINEL2_SR-part-*.parquet`
- Log file:
	- `data/output/et_timeseries_scraper.log`

## Restart behavior (important)

The script is restart-safe.

- If a field file already exists for a dataset, that field is skipped.
- If your run is interrupted, rerun the same command and it continues from remaining fields.

## Main command options

- `--output-dir`: folder for outputs (default: `data/output`)
- `--concurrency`: number of simultaneous API requests (default: `10`)
- `--chunk-size`: fields processed per chunk (default: `2`)
- `--limit-fields`: process only first N fields (default: `0`, meaning all)
- `--verify`: check API metadata before download
- `--log-level`: `DEBUG`, `INFO`, `WARNING`, `ERROR`
- `--no-log-file`: do not write a log file

## Expected columns in output

Each field-level parquet contains:

- `field_id`
- `year`
- `month`
- Variable statistics such as `NDVI_mean`, `NDVI_max`, `et_eemetric_mean`, `et_eemetric_max`, etc.

## Troubleshooting

### Error: Missing CLIMATE_ENGINE_API_KEY

Set the API key in your terminal or `.env` file, then rerun.

### Slow run or API rate issues

Lower concurrency, for example:

```bash
python ag_fields_timeseries_api.py --output-dir data/output --concurrency 5 --chunk-size 10
```

### Some fields fail, others succeed

This can happen with network/API issues. The script logs failed fields and still saves successful ones. Rerun the same command to retry remaining fields.

## Files you will use most

- Main downloader script: `ag_fields_timeseries_api.py`
- HTTP retry helpers: `scrape_utils.py`
- Python dependencies: `requirements.txt`

## Reproducible workflow recommendation

For reporting or publication workflows:

1. Record the command you ran.
2. Keep a copy of `data/output/et_timeseries_scraper.log`.
3. Archive the resulting `data/output/combined` files.