"""Pure, deterministic Spark transformations.

Given the same inputs (and the same `now` for the window), every function
returns the same DataFrame. That determinism is what makes hourly re-runs
idempotent: IDs are stable hashes of business keys, and the merge rule is
a pure function of the unioned data.
"""
from __future__ import annotations

from datetime import UTC, datetime, timedelta

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Spark's hash() is MurmurHash3 32-bit; pmod against this keeps IDs >= 0.
_MAX_ID = 2_147_483_647

CITIES_INPUT_SCHEMA = StructType([
    StructField("city_name", StringType(), False),
    StructField("country_code", StringType(), False),
    StructField("country_name", StringType(), False),
    StructField("country_population", IntegerType(), True),
])

CITIES_SCHEMA = StructType([
    StructField("city_id", IntegerType(), False),
    StructField("city_name", StringType(), False),
    StructField("country_code", StringType(), False),
    StructField("country_name", StringType(), False),
    StructField("country_population", IntegerType(), True),
])

WEATHER_INPUT_SCHEMA = StructType([
    StructField("city_id", IntegerType(), False),
    StructField("timestamp_utc", TimestampType(), False),
    StructField("temperature_celsius", FloatType(), True),
    StructField("is_forecast", BooleanType(), False),
    StructField("source_priority", IntegerType(), True),   # 0 = archive, 1 = forecast
])

WEATHER_SCHEMA = StructType([
    StructField("weather_id", IntegerType(), False),
    StructField("city_id", IntegerType(), False),
    StructField("timestamp_utc", TimestampType(), False),
    StructField("is_forecast", BooleanType(), False),
    StructField("temperature_celsius", FloatType(), True),
])


def _stable_id(*cols: F.Column) -> F.Column:
    """Deterministic, non-negative integer key from business attributes."""
    return F.pmod(F.hash(F.concat_ws("|", *cols)), F.lit(_MAX_ID)).cast("int")


def build_cities_df(spark: SparkSession, countries: list[dict]) -> DataFrame:
    """Dimension table. `countries` is the output of fetch_eu_country_metadata."""
    rows = [
        {k: c.get(k) for k in ("city_name", "country_code", "country_name", "country_population")}
        for c in countries
    ]
    df = spark.createDataFrame(rows, schema=CITIES_INPUT_SCHEMA)
    df = df.withColumn(
        "city_id",
        _stable_id(F.col("country_code"), F.col("city_name")),
    )
    return df.select(*[f.name for f in CITIES_SCHEMA.fields])


def build_weather_df(spark: SparkSession, records: list[dict]) -> DataFrame:
    """Fact table. `records` are dicts with city_id, timestamp_utc,
    temperature_celsius, is_forecast, and optionally source_priority."""
    # Default source_priority so callers that don't care still produce a
    # deterministic tie-break (see merge_and_window).
    records = [{**r, "source_priority": r.get("source_priority", 0)} for r in records]

    df = spark.createDataFrame(records, schema=WEATHER_INPUT_SCHEMA)
    df = df.withColumn(
        "weather_id",
        _stable_id(
            F.col("city_id").cast("string"),
            F.col("timestamp_utc").cast("long").cast("string"),
        ),
    )
    # Keep source_priority through the transform so merge_and_window can use it
    # as a deterministic tie-break. merge_and_window drops it before returning.
    return df.select(*[f.name for f in WEATHER_SCHEMA.fields], "source_priority")


def merge_and_window(weather_df: DataFrame, now: datetime | None = None) -> DataFrame:
    """Resolve forecast/actual overlap and trim to the rolling window.

    Rules, in order:
      1. Drop rows with no temperature (Open-Meteo returns nulls at the
         archive/forecast boundary; a null must not mask a real reading).
      2. For each (city_id, timestamp_utc), keep one row:
           a. actuals (is_forecast=false) beat forecasts;
           b. within the same class, archive (source_priority=0) beats the
              forecast endpoint's `past_days` analysis (source_priority=1);
           c. as a final tie-break, prefer the non-null temperature.
      3. Keep only [now - 90 days, now + 7 days].
    """
    now = now or datetime.now(UTC)
    start = now - timedelta(days=90)
    end = now + timedelta(days=7)

    dedup = Window.partitionBy("city_id", "timestamp_utc").orderBy(
        F.col("is_forecast").asc(),
        F.col("source_priority").asc(),
        F.col("temperature_celsius").asc_nulls_last(),
    )

    return (
        weather_df
        .filter(F.col("temperature_celsius").isNotNull())
        .withColumn("_rank", F.row_number().over(dedup))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
        .filter(F.col("timestamp_utc").between(F.lit(start), F.lit(end)))
        .select(*[f.name for f in WEATHER_SCHEMA.fields])
    )


def write_parquet(df: DataFrame, path: str) -> None:
    """Single-file overwrite: idempotent by construction."""
    df.coalesce(1).write.mode("overwrite").parquet(path)


def reclassify_forecast_status(
    weather_df: DataFrame,
    now: datetime,
) -> DataFrame:
    """Classify weather rows relative to the current pipeline run."""
    return weather_df.withColumn(
        "is_forecast",
        F.col("timestamp_utc") > F.lit(now),
    )