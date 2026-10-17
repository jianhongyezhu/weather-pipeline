from datetime import UTC, datetime, timedelta

from weather_pipeline.transform import (
    build_cities_df,
    build_weather_df,
    merge_and_window,
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
        "city_id": city_id, "timestamp_utc": when,
        "temperature_celsius": temp, "is_forecast": is_forecast,
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

    rows = merge_and_window(forecast.unionByName(actual), now=NOW).collect()

    assert len(rows) == 1
    assert rows[0].temperature_celsius == 18.5
    assert rows[0].is_forecast is False


def test_null_actual_does_not_mask_forecast(spark):
    when = NOW - timedelta(days=1)
    forecast = _weather(spark, 1, when, 20.0, True)
    actual = _weather(spark, 1, when, None, False)

    rows = merge_and_window(forecast.unionByName(actual), now=NOW).collect()

    assert len(rows) == 1
    assert rows[0].temperature_celsius == 20.0


def test_rolling_window_truncation(spark):
    in_range = _weather(spark, 1, NOW - timedelta(days=1), 10.0, False)
    too_old = _weather(spark, 1, NOW - timedelta(days=91), 10.0, False)
    too_new = _weather(spark, 1, NOW + timedelta(days=8), 10.0, True)

    rows = merge_and_window(in_range.unionByName(too_old).unionByName(too_new), now=NOW).collect()

    assert [r.timestamp_utc for r in rows] == [in_range.collect()[0].timestamp_utc]


def test_rerun_is_idempotent(spark):
    records = [
        {"city_id": 1, "timestamp_utc": NOW - timedelta(hours=2),
         "temperature_celsius": 12.0, "is_forecast": False},
        {"city_id": 1, "timestamp_utc": NOW + timedelta(hours=5),
         "temperature_celsius": 15.5, "is_forecast": True},
    ]
    run_1 = merge_and_window(build_weather_df(spark, records), now=NOW).collect()
    run_2 = merge_and_window(build_weather_df(spark, records), now=NOW).collect()

    assert run_1 == run_2
    assert len({(r.city_id, r.timestamp_utc) for r in run_1}) == len(run_1)

def test_archive_beats_forecast_past_days_on_overlap(spark):
    when = NOW - timedelta(days=2)
    archive = build_weather_df(spark, [{
        "city_id": 1, "timestamp_utc": when,
        "temperature_celsius": 11.0, "is_forecast": False,
        "source_priority": 0,
    }])
    forecast_past = build_weather_df(spark, [{
        "city_id": 1, "timestamp_utc": when,
        "temperature_celsius": 13.0, "is_forecast": False,
        "source_priority": 1,
    }])

    rows = merge_and_window(
        archive.unionByName(forecast_past), now=NOW
    ).collect()

    assert len(rows) == 1
    assert rows[0].temperature_celsius == 11.0