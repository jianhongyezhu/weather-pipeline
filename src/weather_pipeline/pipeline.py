"""Orchestrates one hourly run: extract -> transform -> write."""
from __future__ import annotations

import os
import sys

# Must run BEFORE importing pyspark. Spark's PythonWorkerFactory resolves the
# worker interpreter from PYSPARK_PYTHON and caches it at import time.
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# On Windows, PySpark also requires HADOOP_HOME for Parquet writes.
if sys.platform == "win32":
    _hadoop_home = os.environ.get("HADOOP_HOME") or r"C:\hadoop"
    _hadoop_bin = os.path.join(_hadoop_home, "bin")
    if os.path.isdir(_hadoop_bin):
        os.environ.setdefault("HADOOP_HOME", _hadoop_home)
        os.environ.setdefault("hadoop.home.dir", _hadoop_home)
        if _hadoop_bin not in os.environ.get("PATH", ""):
            os.environ["PATH"] = _hadoop_bin + os.pathsep + os.environ.get("PATH", "")

import logging
import time
from datetime import UTC, datetime, timedelta

import requests
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from weather_pipeline.country_clients import fetch_eu_country_metadata
from weather_pipeline.transform import (
    build_cities_df,
    build_weather_df,
    merge_and_window,
    write_parquet,
)
from weather_pipeline.weather_client import (
    fetch_forecast_weather,
    fetch_historical_weather,
    parse_hourly_weather,
)

load_dotenv()

log = logging.getLogger(__name__)

HISTORY_DAYS = 90
MAX_ATTEMPTS = 5
BACKOFF_BASE_SECONDS = 1        # 1, 2, 4, 8, 16
INTER_CITY_DELAY_SECONDS = 0.25


def _with_retry(fn, *args, **kwargs):
    """Retry transient HTTP failures with exponential backoff.

    Retries 5xx and connection/timeout errors only — a 4xx will never
    succeed on retry. Honours Retry-After when the server provides it.
    Non-HTTP exceptions (ValueError, KeyError, ...) propagate immediately.
    """
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            return fn(*args, **kwargs)
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None
            if status is not None and 400 <= status < 500:
                raise  # client error — retrying won't help
            if attempt == MAX_ATTEMPTS:
                raise
            delay = BACKOFF_BASE_SECONDS * (2 ** (attempt - 1))
            if exc.response is not None:
                retry_after = exc.response.headers.get("Retry-After")
                if retry_after:
                    try:
                        delay = max(delay, int(retry_after))
                    except ValueError:
                        pass  # HTTP-date format; ignore
            log.warning(
                "%s failed (attempt %s/%s, status %s), retrying in %ss",
                fn.__name__, attempt, MAX_ATTEMPTS, status, delay,
            )
            time.sleep(delay)
        except (requests.ConnectionError, requests.Timeout) as exc:
            if attempt == MAX_ATTEMPTS:
                raise
            delay = BACKOFF_BASE_SECONDS * (2 ** (attempt - 1))
            log.warning(
                "%s network error (attempt %s/%s): %s, retrying in %ss",
                fn.__name__, attempt, MAX_ATTEMPTS, exc, delay,
            )
            time.sleep(delay)


def run(spark: SparkSession, output_dir: str = "data",
        now: datetime | None = None) -> tuple:
    """Execute one pipeline run.

    Builds both DataFrames fully in memory before writing anything, so a
    failure in the middle of the weather loop leaves the previous hour's
    Parquet files intact rather than partially overwritten.
    """
    now = now or datetime.now(UTC)

    # --- dimension -----------------------------------------------------------
    countries = _with_retry(fetch_eu_country_metadata)
    cities_df = build_cities_df(spark, countries)
    city_id_by_country = {row.country_code: row.city_id for row in cities_df.collect()}

    # --- fact ----------------------------------------------------------------
    start = (now - timedelta(days=HISTORY_DAYS)).date().isoformat()
    end = now.date().isoformat()

    records: list[dict] = []
    for i, country in enumerate(countries):
        city_id = city_id_by_country[country["country_code"]]

        historical = _with_retry(
            fetch_historical_weather,
            country["latitude"], country["longitude"], start, end,
        )
        forecast = _with_retry(
            fetch_forecast_weather,
            country["latitude"], country["longitude"],
        )

        for parsed, priority in (
            (parse_hourly_weather(historical, is_forecast=False), 0),
            (parse_hourly_weather(forecast, is_forecast=True), 1),
        ):
            for record in parsed:
                records.append({
                    **record,
                    "city_id": city_id,
                    "source_priority": priority,
                })

        log.info("fetched %s (%s records so far)", country["country_code"], len(records))
        if i < len(countries) - 1:
            time.sleep(INTER_CITY_DELAY_SECONDS)

    weather_df = build_weather_df(spark, records)
    weather_df = weather_df.withColumn(
        "is_forecast", F.col("timestamp_utc") > F.lit(now)
    )
    weather_df = merge_and_window(weather_df, now=now)

    # --- write both, only after everything succeeded -------------------------
    write_parquet(cities_df, f"{output_dir}/cities.parquet")
    write_parquet(weather_df, f"{output_dir}/weather.parquet")

    log.info("wrote %s cities and %s weather rows",
             cities_df.count(), weather_df.count())
    return cities_df, weather_df


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    spark = (
        SparkSession.builder
        .appName("weather-pipeline")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    try:
        run(spark, output_dir=os.getenv("OUTPUT_DIR", "data"))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()