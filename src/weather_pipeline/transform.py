"""Spark transformations for the weather pipeline.

Given the same inputs and reference time, the transformations produce the
same logical rows. Stable hashes are compact deterministic IDs, not a proof
of uniqueness, so the output builders validate that distinct business keys
have not collided.
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
    """Deterministic, non-negative 32-bit integer derived from key columns.

    This is not collision-free. Callers must validate IDs against their
    logical business keys before publishing the output.
    """
    return F.pmod(F.hash(*cols), F.lit(_MAX_ID)).cast("int")


def assert_no_id_collisions(
    df: DataFrame,
    id_col: str,
    business_key_cols: list[str],
    entity_name: str,
) -> None:
    """Fail if one generated ID maps to more than one distinct business key.

    Repeated rows for the same business key are allowed; those are handled by
    the pipeline's deduplication rule. This check detects hash collisions,
    but deliberately does not attempt to resolve them.
    """
    collisions = (
        df.groupBy(id_col)
        .agg(
            F.countDistinct(F.struct(*[F.col(c) for c in business_key_cols]))
            .alias("business_key_count")
        )
        .filter(F.col("business_key_count") > 1)
    )
    sample = collisions.limit(1).collect()
    if sample:
        raise ValueError(
            f"{entity_name} ID collision detected: distinct business keys "
            f"map to the same {id_col}={sample[0][id_col]!r}"
        )


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
    result = df.select(*[f.name for f in CITIES_SCHEMA.fields])
    assert_no_id_collisions(
        result, "city_id", ["country_code", "city_name"], "City"
    )
    return result


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
            F.col("timestamp_utc").cast("string"),
        ),
    )
    # Keep source_priority through the transform so merge_and_window can use it
    # as a deterministic tie-break. merge_and_window drops it before returning.
    return df.select(*[f.name for f in WEATHER_SCHEMA.fields], "source_priority")


def reclassify_forecast_status(weather_df: DataFrame, now: datetime) -> DataFrame:
    """Mark timestamps after the run's reference time as forecasts.

    Recomputing this each run allows a previously forecast timestamp to
    become historical without retaining state between runs.
    """
    return weather_df.withColumn(
        "is_forecast", F.col("timestamp_utc") > F.lit(now)
    )


def merge_and_window(weather_df: DataFrame, now: datetime | None = None) -> DataFrame:
    """Resolve forecast/actual overlap and trim to the rolling window.

    Rules, in order:
      1. Drop rows with no temperature (Open-Meteo returns nulls at the
         archive/forecast boundary; a null must not mask a real reading).
      2. For each (city_id, timestamp_utc), keep one row:
           a. actuals (is_forecast=false) beat forecasts;
           b. within the same class, archive (source_priority=0) beats the
              forecast endpoint's `past_days` analysis (source_priority=1).
         If equally preferred rows disagree on temperature, fail rather than
         silently selecting an arbitrary or numerically biased value.
      3. Keep only [now - 90 days, now + 7 days].
    """
    now = now or datetime.now(UTC)
    start = now - timedelta(days=90)
    end = now + timedelta(days=7)

    valid = (
        weather_df
        .filter(F.col("temperature_celsius").isNotNull())
        .filter(F.col("timestamp_utc").between(F.lit(start), F.lit(end)))
    )
    precedence = Window.partitionBy("city_id", "timestamp_utc").orderBy(
        F.col("is_forecast").asc(),
        F.col("source_priority").asc_nulls_last(),
    )

    # Only rows tied at the best precedence can affect the chosen output. If
    # they disagree, choosing a temperature by sort order would hide a data
    # conflict, so fail clearly instead.
    top_precedence = (
        valid.withColumn("_precedence_rank", F.dense_rank().over(precedence))
        .filter(F.col("_precedence_rank") == 1)
    )
    conflicts = (
        top_precedence.groupBy("city_id", "timestamp_utc")
        .agg(F.countDistinct("temperature_celsius").alias("temperature_count"))
        .filter(F.col("temperature_count") > 1)
    )
    conflict = conflicts.limit(1).collect()
    if conflict:
        row = conflict[0]
        raise ValueError(
            "Conflicting temperatures for equally preferred records at "
            f"city_id={row['city_id']}, timestamp_utc={row['timestamp_utc']}"
        )

    dedup = Window.partitionBy("city_id", "timestamp_utc").orderBy(
        F.col("is_forecast").asc(),
        F.col("source_priority").asc_nulls_last(),
    )

    result = (
        valid
        .withColumn("_rank", F.row_number().over(dedup))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
        .select(*[f.name for f in WEATHER_SCHEMA.fields])
    )
    assert_no_id_collisions(
        result,
        "weather_id",
        ["city_id", "timestamp_utc"],
        "Weather",
    )
    return result


def write_parquet(df: DataFrame, path: str) -> None:
    """Overwrite one Parquet dataset.

    This makes an individual output a snapshot; it does not make multiple
    output paths an atomic transaction.
    """
    df.coalesce(1).write.mode("overwrite").parquet(path)
