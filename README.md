# EU Capital Weather Pipeline

This project uses PySpark to collect hourly temperature data for the capital cities of the 27 EU countries.

It uses two APIs:

- **REST Countries** for country details and capital city coordinates.
- **Open-Meteo** for historical and forecast temperatures.

The pipeline produces two Parquet datasets:

| Dataset | Description |
|---|---|
| `cities.parquet` | Capital city and country information |
| `weather.parquet` | Hourly temperatures for the past 90 days and next 7 days |

The pipeline is intended to run once an hour. Each run refreshes the data, handles overlapping historical and forecast records, and writes a new snapshot.

## Getting started

You'll need Python 3.11, Java 17, and a REST Countries API key.

From the project root, run:

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1

pip install -r requirements.txt
pip install -e .

copy .env.example .env
```

Add your API key to `.env`, then run:

```powershell
python -m weather_pipeline.pipeline
```

The output is written to the `data/` directory by default. You can change this by setting `OUTPUT_DIR`.

Spark writes Parquet datasets as directories containing part files. I've used `coalesce(1)` so each output directory contains a single data part file, which is convenient for a dataset of this size.

### Running the tests

```powershell
python -m pytest -q
```

The tests cover the API clients, retry behaviour, ID generation, duplicate handling, rolling-window boundaries, and the transition from forecast to historical data.

API calls are mocked in the tests, so they don't require a live connection.

### Windows setup

I developed and tested the pipeline using Python 3.11 and Java 17 on Windows.

The pipeline sets `PYSPARK_PYTHON` to the active Python interpreter to make sure Spark workers use the same virtual environment.

For Parquet writes on Windows, I also used Hadoop 3.3.5 native utilities (`winutils.exe` and `hadoop.dll`) under `C:\hadoop\bin`. The pipeline checks for this location, or you can configure `HADOOP_HOME` yourself.

## How the pipeline works

I chose a **full refresh** rather than an incremental load.

With 27 cities and roughly 97 days of hourly data, the dataset is small enough to fetch and rebuild each time. It also makes the behaviour easier to reason about when forecasts change.

Each run:

1. Fetches the EU countries and their capital cities.
2. Gets historical and forecast temperatures for each capital.
3. Combines the weather records and removes null temperatures.
4. Keeps records within the rolling 90-day historical and 7-day future window.
5. Resolves duplicate city-hour records, giving