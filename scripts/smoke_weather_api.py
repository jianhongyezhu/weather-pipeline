from weather_pipeline.weather_client import (
    fetch_forecast_weather,
    fetch_historical_weather,
)

latitude = 48.2
longitude = 16.37

historical = fetch_historical_weather(
    latitude,
    longitude,
    "2026-07-19",
    "2026-10-16",
)

forecast = fetch_forecast_weather(latitude, longitude)

print("Historical timestamps:", len(historical["hourly"]["time"]))
print("Historical first timestamp:", historical["hourly"]["time"][0])

print("Forecast timestamps:", len(forecast["hourly"]["time"]))
print("Forecast first timestamp:", forecast["hourly"]["time"][0])


from weather_pipeline.weather_client import parse_hourly_weather

historical_records = parse_hourly_weather(historical, is_forecast=False)
forecast_records = parse_hourly_weather(forecast, is_forecast=True)

print(historical_records[0])
print(forecast_records[0])