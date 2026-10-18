# EU Capital Weather Pipeline

PySpark pipeline that fetches EU capital city metadata from **REST Countries**
and hourly temperatures from **Open-Meteo**, then writes two Parquet files:

| File | Contents |
|------|----------|
| `data/cities.parquet` | Dimension table: one row per EU capital |
| `data/weather.parquet` | Fact table: last 90 days of history + next 7 days of forecast |

The pipeline is designed to run **once per hour**. It rebuilds a rolling
snapshot on each run, deduplicates each city-hour, and allows updated forecasts
to replace earlier forecasts. Identical inputs and a fixed reference time
produce equivalent logical rows; live runs can differ because forecasts and
the rolling-window boundaries change.

## How to run

Requirements: **Python 3.11**, **Java 17** on `PATH`, and a
`REST_COUNTRIES_API_KEY` (obtained from restcountries.com).

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
pip install -e .

copy .env.example .env        # then edit .env and paste your API key
python -m weather_pipeline.pipeline
```

`pip install -e .` registers the `weather_pipeline` package with the venv, so
`python -m weather_pipeline.pipeline` works without setting `PYTHONPATH`.
Outputs default to `./data`; override with `OUTPUT_DIR`.

### Tests

```powershell
pytest -q
```

The suite covers the two API clients (mocked, no network), the transform
logic (dedup precedence, rolling window, idempotency), and runs against a
local `SparkSession` configured in `tests/conftest.py`.

### Secrets

`REST_COUNTRIES_API_KEY` is read from the environment. `.env` is gitignored;
`.env.example` documents the required variable and is safe to commit. In
production, a secrets manager (Key Vault, Secrets Manager, Vault) would
inject the same variable at runtime — the code path does not change.

> **Windows notes.** Two Windows-specific setup steps are needed:
> 1. PySpark 3.5.x's bundled `cloudpickle` is incompatible with Python 3.13+, 
>    and Spark workers must use the venv interpreter rather than the Microsoft 
>    Store `python` alias. Both `pipeline.py` and `conftest.py` set 
>    `PYSPARK_PYTHON` from `sys.executable` at import time to prevent this.
> 2. Writing Parquet requires `winutils.exe` and `hadoop.dll` (Hadoop 3.3.5) 
>    on `PATH`. Place them at `C:\hadoop\bin`. The pipeline will automatically 
>    detect this location and configure `HADOOP_HOME` and `PATH` for you. 
>    (If installed elsewhere, set `HADOOP_HOME` and prepend its `bin` folder 
>    to `PATH` manually).

## Approach: IDs and incremental loading

**Deterministic IDs.** Both keys are hashes of business attributes, not
auto-increments:

- `city_id = pmod(Spark hash(country_code, city_name), 2^31-1)`
- `weather_id = pmod(Spark hash(city_id, timestamp_utc), 2^31-1)`

These are deterministic non-negative integer IDs for a given Spark hash
implementation and normalized input. A finite-width hash is not collision-free,
so the pipeline checks that distinct city keys and distinct output weather keys
do not share an ID; a detected collision fails the run rather than silently
publishing ambiguous IDs. This is a guard, not a collision-resolution scheme.

**Loading strategy: full snapshot, not incremental extraction.**
Neither API supports delta extraction (REST Countries returns a snapshot;
Open-Meteo returns a date range). At ~27 cities × ~97 days × 24 hours ≈ 62k
rows, a full re-fetch is a simple choice for this take-home. The pipeline
deliberately re-fetches each run and applies these rules:

1. All records are unioned into one DataFrame.
2. `row_number()` over `(city_id, timestamp_utc)` keeps exactly one row per
   hour, ordered by `is_forecast` (actuals first), then `source_priority`
  (archive before forecast endpoint). If equally preferred records disagree
  on temperature, the run fails rather than silently choosing a value.
3. The result is filtered to `[now - 90 days, now + 7 days]`.
4. Stable IDs are checked for collisions, then each output is written with
  `.mode("overwrite")` and `coalesce(1)`.

This avoids accumulating duplicates across runs. It does **not** promise that
two live runs have identical content: the upstream forecast can change, and
the rolling window moves with `now`. The idempotency test fixes `now` and the
input data, then compares logical rows rather than file bytes or row order.

**Failure handling.** Failed API calls retry with exponential backoff (5
attempts, retrying HTTP 429 and 5xx responses and honouring integer or HTTP-date `Retry-After` values when valid).
If a required API call fails after retries, extraction aborts before either
output is written. However, the two output writes are independent: if the city
write succeeds and the weather write then fails, the published files can
represent different runs. This small solution does not provide atomic
publication of the pair.

If consumers required a consistent multi-file snapshot, the next step would be
to write both datasets to a new versioned staging directory, validate them,
and publish the completed snapshot through a single commit or pointer update
supported by the target storage system. Two independent file renames alone
would not guarantee atomic publication of the pair.

## How forecasts changing and becoming historical are handled

- **Forecast changes between runs.** The forecast table is rewritten in full
  each hour, so only the most recent forecast is retained. Point-in-time
  forecast history is not kept — see *Production at scale* below.
- **Forecast becomes historical.** `is_forecast` is recomputed from the
  timestamp on every run (`timestamp_utc > now`). When a forecast hour falls
  into the past, it is automatically stored as historical. If the archive API
  later returns a measured value for the same hour, the dedup rule keeps the
  archive row (`source_priority = 0`) because both rows are now non-forecast
  and archive wins the tie-break.
- **Archive lag.** Open-Meteo's archive endpoint lags real time by ~5 days.
  The forecast endpoint is therefore called with `past_days=7`, and those past
  hours are reclassified as historical by the timestamp rule. This closes the
  gap without any extra state.

## Assumptions

- The REST Countries response is treated as the authoritative list of EU
  capitals; a country contributes at most its primary capital.
- Temperatures are requested in Celsius and UTC, so no unit or timezone
  conversion is needed downstream.
- `timestamp_utc` is stored as a Spark timestamp. Values are UTC by
  construction (Open-Meteo is queried with `timezone=UTC`, parsing uses
  `tzinfo=UTC`, and both the production and test Spark sessions set
  `spark.sql.session.timeZone=UTC`); the column name is the contract.
- Rows with a null temperature are dropped: a null must not mask a real
  reading during deduplication.
- Open-Meteo's forecast endpoint returns 7 *calendar* days starting from the
  current day, so forecast coverage ends at 23:00 UTC of day 7 and may be up
  to ~24 hours short of `now + 7×24h`. The observed span is ~96 days rather
  than 97.
- The pipeline is a single-process run (`local[*]`); no scheduler is included,
  by design for this exercise.
- `data/` is regenerated on every run and is not meant to be committed.

## Production at scale

Brief notes; the interview will cover each in more depth.

- Batch Open-Meteo calls (the API accepts comma-separated coordinates) and
  checkpoint per-city responses so a failed call falls back to the last
  successful response rather than aborting the run — i.e. add a **Dead-Letter
  Queue** (Konieczny Ch. 3; Kimball Subsystem 24).
- If per-table transactional writes and incremental updates become necessary,
  consider **Delta Lake** and `MERGE` on `(city_id, timestamp_utc)`. A Delta
  transaction would protect an individual table; two separate tables still
  need a publication strategy if consumers require an atomic pair.
- Partition `weather.parquet` by `country_code` (or `date`) to avoid
  single-file scaling limits.
- To retain the forecast known at each point in time, add a `forecast_run_ts`
  column and stop deduplicating across runs — the table becomes an SCD Type 2
  over forecasts, with `is_forecast` and `valid_from/valid_to` capturing when
  each forecast was superseded.
