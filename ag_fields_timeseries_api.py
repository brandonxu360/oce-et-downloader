#!/usr/bin/env python3
"""
Agricultural fields timeseries scraper (ClimateEngine /timeseries/native/coordinates)

What it does
- Downloads field polygons from AG_FIELDS_URL (GeoJSON)
- For each dataset in DATASETS, fetches timeseries for each field (async, rate-limited)
- Aggregates to monthly mean/max per field, then writes one parquet per field:
    {output_dir}/individual/{dataset}/{field_id}.parquet

Restartability
- If a parquet already exists for a field+dataset, it will be skipped.

Usage (example)
    export CLIMATE_ENGINE_API_KEY="..."
    python ag_fields_timeseries_api.py --output-dir data/output --concurrency 10 --chunk-size 25

Optional
    --limit-fields 3     # for smoke testing
    --verify             # run metadata sanity checks against API
"""

from __future__ import annotations

import argparse
import asyncio
import calendar
import json
import logging
import math
import os
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Sequence, Tuple

import aiohttp
import pandas as pd
from dotenv import load_dotenv
from tqdm.asyncio import tqdm

from scrape_utils import asynchronous_fetch_with_retry, synchronous_fetch_with_retry


# ----------------------------
# Configuration (edit defaults here)
# ----------------------------

DATASETS: List[str] = ["LANDSAT_SR", "OPENET_CONUS", "SENTINEL2_SR"]

# Requested variables for each dataset (by index)
VARIABLES: List[List[str]] = [
    ["NDVI", "MSAVI", "NDWI_NIR_SWIR_Gao"],
    ["et_eemetric", "et_geesebal", "et_disalexi"],
    ["NDVI", "MSAVI", "NDWI_NIR_SWIR_Gao", "NDRE", "BSI"],
]

# Requested year range (inclusive) for each dataset (by index)
YEARS: List[List[int]] = [
    [2008, 2024],
    [2008, 2024],
    [2017, 2024],
]

# Requested months for each dataset (by index)
MONTHS: List[List[int]] = [
    [4, 5, 6, 7, 8, 9],
    [4, 5, 6, 7, 8, 9],
    [4, 5, 6, 7, 8, 9],
]

AG_FIELDS_URL_DEFAULT = (
    "https://wc.bearhive.duckdns.org/weppcloud/runs/copacetic-note/ag-fields/"
    "browse/ag_fields/CSB_2008_2024_Hangman_with_Crop_and_Performance.geojson?raw=true"
)


# ----------------------------
# Logging
# ----------------------------

def setup_logging(output_dir: Path, log_level: str, log_to_file: bool) -> logging.Logger:
    logger = logging.getLogger("climateengine.scraper.timeseries")
    logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))
    logger.propagate = False  # avoid duplicate logs if root logger configured elsewhere

    # Clear existing handlers (helps in notebooks / repeated runs)
    logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s | %(levelname)-8s | %(name)s | %(message)s")

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    if log_to_file:
        output_dir.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(output_dir / "et_timeseries_scraper.log")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


# ----------------------------
# Helpers
# ----------------------------

def require_api_key() -> str:
    load_dotenv()
    api_key = os.environ.get("CLIMATE_ENGINE_API_KEY")
    if not api_key:
        raise RuntimeError(
            "Missing CLIMATE_ENGINE_API_KEY. Set it in your environment or in a .env file."
        )
    return api_key


def build_headers(api_key: str) -> Dict[str, str]:
    return {
        "Accept": "application/json",
        "Authorization": api_key,
    }


def get_finished_field_ids(individual_dir: Path, dataset: str) -> set[str]:
    dataset_dir = individual_dir / dataset
    if not dataset_dir.exists():
        return set()

    finished: set[str] = set()
    for field_file in dataset_dir.glob("*.parquet"):
        finished.add(field_file.stem)  # stems are strings
    return finished


def load_fields_df(ag_fields_url: str, logger: logging.Logger) -> pd.DataFrame:
    logger.info("Downloading fields GeoJSON: %s", ag_fields_url)
    fields_data = synchronous_fetch_with_retry(ag_fields_url)
    fields: List[Dict[str, Any]] = []

    for feature in fields_data.get("features", []):
        properties = feature.get("properties", {})
        geometry = feature.get("geometry", {})
        field_id = properties.get("field_ID")

        fields.append(
            {
                "field_id": field_id,
                "geometry": geometry,
            }
        )

    df = pd.DataFrame(fields)
    # Normalize field_id to string early so comparisons are consistent
    df["field_id"] = df["field_id"].astype(str)
    logger.info("Loaded %d fields", len(df))
    return df


def process_df(raw_df: pd.DataFrame, dataset_index: int) -> pd.DataFrame:
    """
    Convert raw timeseries rows (Date + variables) into monthly mean/max columns.

    Output columns:
      field_id, year, month, <var>_mean, <var>_max
    """
    if raw_df.empty:
        return raw_df

    df = raw_df.copy()

    # Normalize types
    df["field_id"] = df["field_id"].astype(str)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])

    # Filter requested months (years already bounded by API request)
    df = df[df["date"].dt.month.isin(MONTHS[dataset_index])]

    # Avoid sentinel contamination
    df = df.replace(-9999, pd.NA)

    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month

    keys = ["field_id", "date", "year", "month"]
    value_cols = [c for c in df.columns if c not in keys]

    df[value_cols] = df[value_cols].apply(pd.to_numeric, errors="coerce")

    agg_df = (
        df.groupby(["field_id", "year", "month"])[value_cols]
        .agg(["mean", "max"])
        .reset_index()
    )

    # Flatten multi-index columns
    agg_df.columns = [
        f"{col}_{stat}" if stat else col
        for col, stat in agg_df.columns
    ]

    # Round numeric stats
    stat_cols = [c for c in agg_df.columns if c not in ["field_id", "year", "month"]]
    agg_df[stat_cols] = agg_df[stat_cols].round(4)

    return agg_df


# ----------------------------
# Async fetch
# ----------------------------

async def fetch_one_field(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    headers: Mapping[str, str],
    dataset: str,
    variables: Sequence[str],
    start_date: str,
    end_date: str,
    field_id: str,
    coordinates: Any,
) -> Tuple[str, Mapping[str, Any]]:
    payload = await asynchronous_fetch_with_retry(
        session=session,
        url="https://api.climateengine.org/timeseries/native/coordinates",
        semaphore=semaphore,
        headers=headers,
        params={
            "dataset": dataset,
            "variable": ",".join(variables),
            "start_date": start_date,
            "end_date": end_date,
            "area_reducer": "median",
            "coordinates": json.dumps(coordinates),
        },
    )
    return field_id, payload


async def fetch_data(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    headers: Mapping[str, str],
    dataset: str,
    dataset_index: int,
    fields_df: pd.DataFrame,
) -> List[Tuple[str, Mapping[str, Any]]]:
    tasks = []
    start_date = f"{YEARS[dataset_index][0]}-{MONTHS[dataset_index][0]:02d}-01"
    end_month = MONTHS[dataset_index][-1]
    end_year = YEARS[dataset_index][1]
    end_day = calendar.monthrange(end_year, end_month)[1]
    end_date = f"{end_year}-{end_month:02d}-{end_day}"

    for _, row in fields_df.iterrows():
        tasks.append(
            fetch_one_field(
                session=session,
                semaphore=semaphore,
                headers=headers,
                dataset=dataset,
                variables=VARIABLES[dataset_index],
                start_date=start_date,
                end_date=end_date,
                field_id=str(row["field_id"]),
                coordinates=row["geometry"]["coordinates"],
            )
        )

    return await tqdm.gather(*tasks)


def convert_results_to_pandas(
    results: Iterable[Tuple[str, Mapping[str, Any]]]
) -> Dict[str, pd.DataFrame]:
    """Convert API response payloads into raw per-field dataframes."""
    out: Dict[str, pd.DataFrame] = {}

    for field_id, result in results:
        rows: List[Dict[str, Any]] = []

        data_list = result.get("Data") if isinstance(result, dict) else None
        if data_list and len(data_list) > 0:
            timeseries_data = data_list[0].get("Data", [])
            for data_point in timeseries_data:
                row = {
                    "field_id": str(field_id),
                    "date": data_point.get("Date"),
                    **{k: v for k, v in data_point.items() if k != "Date"},
                }
                rows.append(row)

        out[str(field_id)] = pd.DataFrame(rows)

    return out


# ----------------------------
# Optional verification (metadata sanity checks)
# ----------------------------

def verify_metadata(headers: Mapping[str, str], logger: logging.Logger) -> None:
    # Verify variables exist
    for i, dataset in enumerate(DATASETS):
        res = synchronous_fetch_with_retry(
            f"https://api.climateengine.org/metadata/dataset_variables?dataset={dataset}",
            headers=headers,
        )
        api_variables = set(res.get("Data", {}).get("variables", []))
        missing = set(VARIABLES[i]).difference(api_variables)
        if missing:
            logger.warning("%s: API missing requested variables %s", dataset, sorted(missing))
        else:
            logger.info("%s: all requested variables available", dataset)

    # Verify requested dates are within dataset availability
    for i, dataset in enumerate(DATASETS):
        res = synchronous_fetch_with_retry(
            f"https://api.climateengine.org/metadata/dataset_dates?dataset={dataset}",
            headers=headers,
        )
        data = res.get("Data", {}) or {}
        date_min = int(str(data.get("min", "0000"))[:4])
        date_max = int(str(data.get("max", "0000"))[:4])
        req_min, req_max = YEARS[i]
        ok = (req_min >= date_min) and (req_max <= date_max)
        logger.info(
            "%s: %s (available: %d-%d, requested: %d-%d)",
            dataset,
            "✓" if ok else "✗",
            date_min,
            date_max,
            req_min,
            req_max,
        )


# ----------------------------
# Main
# ----------------------------

async def main() -> None:
    parser = argparse.ArgumentParser(description="ClimateEngine ag fields timeseries scraper")
    parser.add_argument("--ag-fields-url", default=AG_FIELDS_URL_DEFAULT)
    parser.add_argument("--output-dir", default="data/output")
    parser.add_argument("--chunk-size", type=int, default=2)
    parser.add_argument("--concurrency", type=int, default=10)
    parser.add_argument("--limit-fields", type=int, default=0, help="0 = no limit (process all)")
    parser.add_argument("--verify", action="store_true", help="Run metadata sanity checks before scraping")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    parser.add_argument("--no-log-file", action="store_true", help="Disable writing logs to a file")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    individual_dir = output_dir / "individual"

    logger = setup_logging(output_dir=output_dir, log_level=args.log_level, log_to_file=not args.no_log_file)

    api_key = require_api_key()
    headers = build_headers(api_key)

    if args.verify:
        verify_metadata(headers=headers, logger=logger)

    # Load fields
    fields_df = load_fields_df(args.ag_fields_url, logger)

    if args.limit_fields and args.limit_fields > 0:
        fields_df = fields_df.head(args.limit_fields)
        logger.warning("Limit enabled: processing only %d fields", len(fields_df))

    semaphore = asyncio.Semaphore(args.concurrency)

    timeout = aiohttp.ClientTimeout(total=None)
    async with aiohttp.ClientSession(raise_for_status=True, timeout=timeout) as session:
        for dataset_index, dataset in enumerate(DATASETS):
            logger.info("%s: starting...", dataset)

            finished = get_finished_field_ids(individual_dir, dataset)
            pending_fields = fields_df[~fields_df["field_id"].isin(finished)]
            num_pending = len(pending_fields)

            if num_pending == 0:
                logger.info("%s: nothing to do (all fields already processed)", dataset)
                continue

            chunks = math.ceil(num_pending / args.chunk_size)
            logger.info("%s: %d pending fields, %d chunks (chunk size=%d)", dataset, num_pending, chunks, args.chunk_size)

            dataset_out_dir = individual_dir / dataset
            dataset_out_dir.mkdir(parents=True, exist_ok=True)

            for chunk_num in range(chunks):
                start = chunk_num * args.chunk_size
                end = min((chunk_num + 1) * args.chunk_size, num_pending)
                chunk_df = pending_fields.iloc[start:end]

                logger.info("%s: chunk %d/%d (%d fields)", dataset, chunk_num + 1, chunks, len(chunk_df))

                results = await fetch_data(
                    session=session,
                    semaphore=semaphore,
                    headers=headers,
                    dataset=dataset,
                    dataset_index=dataset_index,
                    fields_df=chunk_df,
                )
                raw_dfs = convert_results_to_pandas(results)

                for field_id, field_df in raw_dfs.items():
                    processed_df = process_df(field_df, dataset_index)
                    output_file = dataset_out_dir / f"{field_id}.parquet"
                    processed_df.to_parquet(output_file, engine="pyarrow", compression="snappy", index=False)

            logger.info("%s: finished", dataset)


if __name__ == "__main__":
    asyncio.run(main())
