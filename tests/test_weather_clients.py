from datetime import UTC, datetime
from unittest.mock import Mock

import pytest
import requests

from weather_pipeline.weather_client import (
    fetch_forecast_weather,
    fetch_historical_weather,
    parse_hourly_weather,
)


def test_parse_hourly_weather_builds_records():
    payload = {
        "hourly": {
            "time": ["2026-07-19T00:00", "2026-07-19T01:00"],
            "temperature_2m": [18.5, None],
        }
    }

    assert parse_hourly_weather(payload, is_forecast=True) == [
        {
            "timestamp_utc": datetime(2026, 7, 19, 0, 0, tzinfo=UTC),
            "temperature_celsius": 18.5,
            "is_forecast": True,
        },
        {
            "timestamp_utc": datetime(2026, 7, 19, 1, 0, tzinfo=UTC),
            "temperature_celsius": None,
            "is_forecast": True,
        },
    ]


def test_parse_hourly_weather_raises_on_length_mismatch():
    payload = {
        "hourly": {
            "time": ["2026-07-19T00:00", "2026-07-19T01:00"],
            "temperature_2m": [18.5],
        }
    }

    with pytest.raises(ValueError, match="counts do not match"):
        parse_hourly_weather(payload, is_forecast=False)


def test_fetch_historical_weather_sends_expected_request(monkeypatch):
    fake_response = Mock(json=Mock(return_value={"hourly": {}}))
    fake_get = Mock(return_value=fake_response)
    monkeypatch.setattr("weather_pipeline.weather_client.requests.get", fake_get)

    result = fetch_historical_weather(48.2, 16.37, "2026-07-19", "2026-10-16")

    assert result == {"hourly": {}}
    fake_get.assert_called_once_with(
        "https://archive-api.open-meteo.com/v1/archive",
        params={
            "latitude": 48.2,
            "longitude": 16.37,
            "hourly": "temperature_2m",
            "start_date": "2026-07-19",
            "end_date": "2026-10-16",
            "timezone": "UTC",
            "temperature_unit": "celsius",
        },
        timeout=30,
    )


def test_fetch_forecast_weather_sends_expected_request(monkeypatch):
    fake_response = Mock(json=Mock(return_value={"hourly": {}}))
    fake_get = Mock(return_value=fake_response)
    monkeypatch.setattr("weather_pipeline.weather_client.requests.get", fake_get)

    fetch_forecast_weather(48.2, 16.37)

    fake_get.assert_called_once_with(
        "https://api.open-meteo.com/v1/forecast",
        params={
            "latitude": 48.2,
            "longitude": 16.37,
            "hourly": "temperature_2m",
            "forecast_days": 7,
            "past_days": 7,
            "timezone": "UTC",
            "temperature_unit": "celsius",
        },
        timeout=30,
    )


def test_fetch_forecast_weather_raises_on_http_error(monkeypatch):
    fake_response = Mock()
    fake_response.raise_for_status.side_effect = requests.HTTPError("Server error")
    monkeypatch.setattr(
        "weather_pipeline.weather_client.requests.get",
        Mock(return_value=fake_response),
    )

    with pytest.raises(requests.HTTPError):
        fetch_forecast_weather(48.2, 16.37)