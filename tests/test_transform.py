from datetime import UTC, datetime, timedelta

import pytest
from pyspark.sql import functions as F

from weather_pipeline.transform import (
    assert_no_id_collisions,
    build_cities_df,
    build_weather_df,
    merge_and_window,
    reclassify_forecast_status,
)

NOW = datetime(2026, 10, 17, 12, 0, tzinfo=UTC)

COUNTRIES = [
    {"city_name": "Vienna", "country_code": "AT", "country_name": "Austria",
     "country_population": 9217573, "latitude": 48.2, "longitude": 16.37},
    {"city_name": "Brussels", "country_code": "BE", "country_name": "Belgium",
     "country_population": 11942772, "latitude": 50.83, "longitude": 4.33},
]


def _weather(spark, city_id, when, temp, is_forecast):
    return build_weather_df(spark, [{
        "city_id": city_id,
        "timestamp_utc": when,
        "temperature_celsius": temp,
        "is_forecast": is_forecast,
    }])


def test_city_ids_are_deterministic_and_non_negative(spark):
    first = build_cities_df(spark, COUNTRIES).collect()
    second = build_cities_df(spark, COUNTRIES).collect()

    assert first == second
    assert all(row.city_id >= 0 for row in first)
    assert len({row.city_id for row in first}) == len(first)


def test_actual_beats_forecast_for_same_hour(spark):
    when = NOW - timedelta(days=1)
    forecast = _weather(spark, 1, when, 20.0, True)
    actual = _weather(spark, 1, when, 18.5, False)

    rows = merge_and_window(
        forecast.unionByName(actual), now=NOW
    ).collect()

    assert len(rows) == 1
    assert rows[0].temperature_celsius == 18.5
    assert rows[0].is_forecast is False


def test_null_actual_does_not_mask_forecast(spark):
    when = NOW - timedelta(days=1)
    forecast = _weather(spark, 1, when, 20.0, True)
    actual = _weather(spark, 1, when, None, False)

    rows = merge_and_window(
        forecast.unionByName(actual), now=NOW
    ).collect()

    assert len(rows) == 1
    assert rows[0].temperature_celsius == 20.0


def test_rolling_window_truncation(spark):
    in_range = _weather(
        spark, 1, NOW - timedelta(days=1), 10.0, False
    )
    too_old = _weather(
        spark, 1, NOW - timedelta(days=91), 10.0, False
    )
    too_new = _weather(
        spark, 1, NOW + timedelta(days=8), 10.0, True
    )

    rows = merge_and_window(
        in_range.unionByName(too_old).unionByName(too_new),
        now=NOW,
    ).collect()

    assert [r.timestamp_utc for r in rows] == [
        in_range.collect()[0].timestamp_utc
    ]


def test_rolling_window_includes_exact_boundaries_and_excludes_outside(spark):
    start = NOW - timedelta(days=90)
    end = NOW + timedelta(days=7)

    records = [
        {
            "city_id": 1,
            "timestamp_utc": start - timedelta(microseconds=1),
            "temperature_celsius": 1.0,
            "is_forecast": False,
        },
        {
            "city_id": 1,
            "timestamp_utc": start,
            "temperature_celsius": 2.0,
            "is_forecast": False,
        },
        {
            "city_id": 1,
            "timestamp_utc": end,
            "temperature_celsius": 3.0,
            "is_forecast": True,
        },
        {
            "city_id": 1,
            "timestamp_utc": end + timedelta(microseconds=1),
            "temperature_celsius": 4.0,
            "is_forecast": True,
        },
    ]

    result = merge_and_window(
        build_weather_df(spark, records),
        now=NOW,
    )

    actual = {
        row.timestamp_epoch
        for row in (
            result
            .select(
                F.col("timestamp_utc")
                .cast("long")
                .alias("timestamp_epoch")
            )
            .collect()
        )
    }

    expected = {
        int(start.timestamp()),
        int(end.timestamp()),
    }

    assert actual == expected

    rows = result.select("temperature_celsius").collect()

    assert len(rows) == 2
    assert {row.temperature_celsius for row in rows} == {2.0, 3.0}


def test_rerun_is_idempotent(spark):
    records = [
        {
            "city_id": 1,
            "timestamp_utc": NOW - timedelta(hours=2),
            "temperature_celsius": 12.0,
            "is_forecast": False,
        },
        {
            "city_id": 1,
            "timestamp_utc": NOW + timedelta(hours=5),
            "temperature_celsius": 15.5,
            "is_forecast": True,
        },
    ]

    run_1 = merge_and_window(
        build_weather_df(spark, records), now=NOW
    ).collect()
    run_2 = merge_and_window(
        build_weather_df(spark, records), now=NOW
    ).collect()

    assert run_1 == run_2
    assert len({
        (r.city_id, r.timestamp_utc) for r in run_1
    }) == len(run_1)


def test_forecast_timestamp_becomes_historical_as_reference_time_advances(spark):
    forecast_time = NOW + timedelta(hours=1)

    forecast = build_weather_df(spark, [{
        "city_id": 1,
        "timestamp_utc": forecast_time,
        "temperature_celsius": 15.0,
        "is_forecast": True,
    }])

    first_run = reclassify_forecast_status(
        forecast, NOW
    ).collect()
    second_run = reclassify_forecast_status(
        forecast, NOW + timedelta(hours=2)
    ).collect()

    assert first_run[0].is_forecast is True
    assert second_run[0].is_forecast is False


def test_conflicting_equal_precedence_temperatures_fail(spark):
    when = NOW - timedelta(hours=1)

    first = build_weather_df(spark, [{
        "city_id": 1,
        "timestamp_utc": when,
        "temperature_celsius": 10.0,
        "is_forecast": False,
        "source_priority": 0,
    }])
    second = build_weather_df(spark, [{
        "city_id": 1,
        "timestamp_utc": when,
        "temperature_celsius": 12.0,
        "is_forecast": False,
        "source_priority": 0,
    }])

    with pytest.raises(ValueError, match="Conflicting temperatures"):
        merge_and_window(
            first.unionByName(second), now=NOW
        ).collect()


def test_id_collision_check_fails_for_distinct_business_keys(spark):
    df = spark.createDataFrame([
        (7, "AT", "Vienna"),
        (7, "BE", "Brussels"),
    ], ["city_id", "country_code", "city_name"])

    with pytest.raises(ValueError, match="City ID collision detected"):
        assert_no_id_collisions(
            df,
            "city_id",
            ["country_code", "city_name"],
            "City",
        )


def test_archive_beats_forecast_past_days_on_overlap(spark):
    when = NOW - timedelta(days=2)

    archive = build_weather_df(spark, [{
        "city_id": 1,
        "timestamp_utc": when,
        "temperature_celsius": 11.0,
        "is_forecast": False,
        "source_priority": 0,
    }])
    forecast_past = build_weather_df(spark, [{
        "city_id": 1,
        "timestamp_utc": when,
        "temperature_celsius": 13.0,
        "is_forecast": False,
        "source_priority": 1,
    }])

    rows = merge_and_window(
        archive.unionByName(forecast_past),
        now=NOW,
    ).collect()

    assert len(rows) == 1
    assert rows[0].temperature_celsius == 11.0


def test_weather_ids_are_sequential_and_deterministic(spark):
    records = [
        {
            "city_id": 2,
            "timestamp_utc": NOW + timedelta(hours=1),
            "temperature_celsius": 20.0,
            "is_forecast": True,
        },
        {
            "city_id": 1,
            "timestamp_utc": NOW - timedelta(hours=1),
            "temperature_celsius": 10.0,
            "is_forecast": False,
        },
        {
            "city_id": 1,
            "timestamp_utc": NOW + timedelta(hours=2),
            "temperature_celsius": 12.0,
            "is_forecast": True,
        },
    ]

    first = merge_and_window(
        build_weather_df(spark, records),
        now=NOW,
    )
    second = merge_and_window(
        build_weather_df(spark, list(reversed(records))),
        now=NOW,
    )

    first_rows = first.orderBy(
        "city_id", "timestamp_utc"
    ).collect()
    second_rows = second.orderBy(
        "city_id", "timestamp_utc"
    ).collect()

    assert first_rows == second_rows
    assert [row.weather_id for row in first_rows] == [1, 2, 3]
    assert len({
        row.weather_id for row in first_rows
    }) == len(first_rows)

    assert dict(first.dtypes)["weather_id"] == "int"


def test_weather_ids_can_shift_when_snapshot_changes(spark):
    retained = {
        "city_id": 1,
        "timestamp_utc": NOW - timedelta(hours=2),
        "temperature_celsius": 12.0,
        "is_forecast": False,
    }
    earlier = {
        "city_id": 1,
        "timestamp_utc": NOW - timedelta(hours=3),
        "temperature_celsius": 11.0,
        "is_forecast": False,
    }

    first = merge_and_window(
        build_weather_df(spark, [retained]),
        now=NOW,
    ).collect()

    second = merge_and_window(
        build_weather_df(spark, [earlier, retained]),
        now=NOW,
    ).collect()

    assert first[0].weather_id == 1

    retained_in_second = next(
        row for row in second
        if row.temperature_celsius == 12.0
    )

    assert retained_in_second.weather_id == 2
