from datetime import UTC, datetime

import requests

ARCHIVE_API_URL = "https://archive-api.open-meteo.com/v1/archive"
FORECAST_API_URL = "https://api.open-meteo.com/v1/forecast"


def fetch_historical_weather(latitude, longitude, start_date, end_date):
    """Fetch hourly historical temperatures for one location."""
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": "temperature_2m",
        "start_date": start_date,
        "end_date": end_date,
        "timezone": "UTC",
        "temperature_unit": "celsius",
    }

    response = requests.get(
        ARCHIVE_API_URL,
        params=params,
        timeout=30,
    )
    response.raise_for_status()

    return response.json()


def fetch_forecast_weather(latitude, longitude):
    """Fetch hourly temperatures for one location.

    Request eight calendar days so a run at any time of day can cover
    the full rolling seven-day future window. The transformation layer
    trims the response to now + 7 days.

    Also request seven past calendar days because the archive endpoint
    can lag behind real time. The pipeline classifies those records
    relative to the run timestamp.
    """
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": "temperature_2m",
        "forecast_days": 8,
        "past_days": 7,
        "timezone": "UTC",
        "temperature_unit": "celsius",
    }

    response = requests.get(
        FORECAST_API_URL,
        params=params,
        timeout=30,
    )
    response.raise_for_status()

    return response.json()


def parse_hourly_weather(response, is_forecast):
    """Convert Open-Meteo hourly arrays into individual records."""
    hourly = response["hourly"]
    timestamps = hourly["time"]
    temperatures = hourly["temperature_2m"]

    if len(timestamps) != len(temperatures):
        raise ValueError("Timestamp and temperature counts do not match")

    records = []

    for timestamp, temperature in zip(timestamps, temperatures):
        parsed_timestamp = datetime.strptime(
            timestamp, "%Y-%m-%dT%H:%M"
        ).replace(tzinfo=UTC)

        records.append({
            "timestamp_utc": parsed_timestamp,
            "temperature_celsius": (
                float(temperature) if temperature is not None else None
            ),
            "is_forecast": is_forecast,
        })

    return records
