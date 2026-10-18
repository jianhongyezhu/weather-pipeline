import pytest
import requests

from weather_pipeline import pipeline


def _http_error(status_code, retry_after=None):
    response = requests.Response()
    response.status_code = status_code
    if retry_after is not None:
        response.headers["Retry-After"] = retry_after
    return requests.HTTPError(f"HTTP {status_code}", response=response)


def test_retry_retries_http_429_and_honours_retry_after(monkeypatch):
    sleeps = []
    monkeypatch.setattr(pipeline.time, "sleep", sleeps.append)
    attempts = iter([_http_error(429, "3"), "ok"])

    def request():
        result = next(attempts)
        if isinstance(result, Exception):
            raise result
        return result

    assert pipeline._with_retry(request) == "ok"
    assert sleeps == [3]


def test_retry_does_not_retry_non_retryable_4xx(monkeypatch):
    sleeps = []
    calls = []
    monkeypatch.setattr(pipeline.time, "sleep", sleeps.append)

    def request():
        calls.append(1)
        raise _http_error(400)

    with pytest.raises(requests.HTTPError):
        pipeline._with_retry(request)

    assert len(calls) == 1
    assert sleeps == []


def test_retry_exhaustion_propagates_last_server_error(monkeypatch):
    sleeps = []
    calls = []
    monkeypatch.setattr(pipeline.time, "sleep", sleeps.append)

    def request():
        calls.append(1)
        raise _http_error(503)

    with pytest.raises(requests.HTTPError):
        pipeline._with_retry(request)

    assert len(calls) == pipeline.MAX_ATTEMPTS
    assert len(sleeps) == pipeline.MAX_ATTEMPTS - 1
