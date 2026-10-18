"""Spark transformations for the weather pipeline.

Given the same inputs and reference time, the transformations produce the
same logical rows.

City IDs are deterministic hashes with collision validation.
Weather IDs are deterministic sequential integers within each snapshot,
assigned after filtering and deduplication.
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

# Maximum positive value of a signed 32-bit integer.
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
    StructField("source_priority", IntegerType(), True),
])

WEATHER_SCHEMA = StructType([
    StructField("weather_id", IntegerType(), False),
    StructField("city_id", IntegerType(), False),
    StructField("timestamp_utc", TimestampType(), False),
    StructField("is_forecast", BooleanType(), False),
    StructField("temperature_celsius", FloatType(), True),
])


def _stable_id(*cols: F.Column) -> F.Column:
    """Create a deterministic, non-negative hash-based integer ID.

    Spark's hash() uses a 32-bit hash, which can collide. Callers must
    validate generated IDs against their business keys.
    """
    return F.pmod(
        F.hash(*cols),
        F.lit(_MAX_ID),
    ).cast("int")


def assert_no_id_collisions(
    df: DataFrame,
    id_col: str,
    business_key_cols: list[str],
    entity_name: str,
) -> None:
    """Fail when distinct business keys share a generated ID.

    Repeated rows for the same business key are allowed.
    """
    collisions = (
        df.groupBy(id_col)
        .agg(
            F.countDistinct(
                F.struct(*[
                    F.col(c) for c in business_key_cols
                ])
            ).alias("business_key_count")
        )
        .filter(F.col("business_key_count") > 1)
    )

    sample = collisions.limit(1).collect()

    if sample:
        raise ValueError(
            f"{entity_name} ID collision detected: distinct business keys "
            f"map to the same {id_col}={sample[0][id_col]!r}"
        )


def build_cities_df(
    spark: SparkSession,
    countries: list[dict],
) -> DataFrame:
    """Build the city dimension from country metadata."""
    rows = [
        {
            k: c.get(k)
            for k in (
                "city_name",
                "country_code",
                "country_name",
                "country_population",
            )
        }
        for c in countries
    ]

    df = spark.createDataFrame(
        rows,
        schema=CITIES_INPUT_SCHEMA,
    )

    df = df.withColumn(
        "city_id",
        _stable_id(
            F.col("country_code"),
            F.col("city_name"),
        ),
    )

    result = df.select(
        *[field.name for field in CITIES_SCHEMA.fields]
    )

    assert_no_id_collisions(
        result,
        "city_id",
        ["country_code", "city_name"],
        "City",
    )

    return result


def build_weather_df(
    spark: SparkSession,
    records: list[dict],
) -> DataFrame:
    """Build raw weather rows without assigning weather IDs.

    Weather IDs must be assigned after deduplication and rolling-window
    filtering, so each published row receives exactly one unique ID.
    """
    records = [
        {
            **record,
            "source_priority": record.get("source_priority", 0),
        }
        for record in records
    ]

    return spark.createDataFrame(
        records,
        schema=WEATHER_INPUT_SCHEMA,
    )


def reclassify_forecast_status(
    weather_df: DataFrame,
    now: datetime,
) -> DataFrame:
    """Classify rows relative to the current pipeline run.

    A previously forecast timestamp becomes historical once it is no
    longer in the future.
    """
    return weather_df.withColumn(
        "is_forecast",
        F.col("timestamp_utc") > F.lit(now),
    )


def merge_and_window(
    weather_df: DataFrame,
    now: datetime | None = None,
) -> DataFrame:
    """Resolve overlapping weather records and apply the rolling window.

    Rules:
      1. Exclude null temperatures.
      2. Keep one row per (city_id, timestamp_utc).
      3. Prefer historical rows over forecasts.
      4. Prefer archive data over forecast-endpoint past-day data.
      5. Reject conflicting equally preferred temperatures.
      6. Retain [now - 90 days, now + 7 days].
      7. Assign sequential weather IDs to the final snapshot.

    Sequential IDs are unique and deterministic for a fixed snapshot.
    They are not guaranteed to remain stable as the window moves.
    """
    now = now or datetime.now(UTC)

    start = now - timedelta(days=90)
    end = now + timedelta(days=7)

    valid = (
        weather_df
        .filter(
            F.col("temperature_celsius").isNotNull()
        )
        .filter(
            F.col("timestamp_utc").between(
                F.lit(start),
                F.lit(end),
            )
        )
    )

    precedence = (
        Window
        .partitionBy("city_id", "timestamp_utc")
        .orderBy(
            F.col("is_forecast").asc(),
            F.col("source_priority").asc_nulls_last(),
        )
    )

    # Inspect only rows tied at the best precedence.
    top_precedence = (
        valid
        .withColumn(
            "_precedence_rank",
            F.dense_rank().over(precedence),
        )
        .filter(
            F.col("_precedence_rank") == 1
        )
    )

    conflicts = (
        top_precedence
        .groupBy("city_id", "timestamp_utc")
        .agg(
            F.countDistinct(
                "temperature_celsius"
            ).alias("temperature_count")
        )
        .filter(
            F.col("temperature_count") > 1
        )
    )

    conflict = conflicts.limit(1).collect()

    if conflict:
        row = conflict[0]
        raise ValueError(
            "Conflicting temperatures for equally preferred records at "
            f"city_id={row['city_id']}, "
            f"timestamp_utc={row['timestamp_utc']}"
        )

    dedup = (
        Window
        .partitionBy("city_id", "timestamp_utc")
        .orderBy(
            F.col("is_forecast").asc(),
            F.col("source_priority").asc_nulls_last(),
        )
    )

    deduplicated = (
        valid
        .withColumn(
            "_rank",
            F.row_number().over(dedup),
        )
        .filter(
            F.col("_rank") == 1
        )
        .drop("_rank", "source_priority")
    )

    # row_number() produces unique values from 1 through the row count.
    # Since business keys are unique after deduplication, ordering by
    # (city_id, timestamp_utc) is deterministic for a fixed snapshot.
    id_window = Window.orderBy(
        F.col("city_id").asc(),
        F.col("timestamp_utc").asc(),
    )

    result = deduplicated.withColumn(
        "weather_id",
        F.row_number().over(id_window).cast("int"),
    )

    return result.select(
        *[field.name for field in WEATHER_SCHEMA.fields]
    )


def write_parquet(
    df: DataFrame,
    path: str,
) -> None:
    """Overwrite one Parquet snapshot.

    Individual output paths are not an atomic multi-table transaction.
    """
    df.coalesce(1).write.mode("overwrite").parquet(path)
