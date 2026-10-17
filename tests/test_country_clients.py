from unittest.mock import MagicMock, Mock

import pytest
import requests

from weather_pipeline.country_clients import fetch_eu_country_metadata

AUSTRIA = {
    "names": {"common": "Austria"},
    "codes": {"alpha_2": "AT"},
    "population": 9217573,
    "capitals": [{
        "name": "Vienna",
        "attributes": {"primary": True},
        "coordinates": {"lat": 48.2, "lng": 16.37},
    }],
}

BELGIUM = {
    "names": {"common": "Belgium"},
    "codes": {"alpha_2": "BE"},
    "population": 11942772,
    "capitals": [{
        "name": "Brussels",
        "attributes": {"primary": True},
        "coordinates": {"lat": 50.83, "lng": 4.33},
    }],
}


def _mock_session_returning(*responses):
    """A MagicMock Session whose .get() yields `responses` in order."""
    session = MagicMock()
    session.__enter__.return_value = session  # `with session as s:` → s is session
    session.get.side_effect = list(responses)
    return session


def _mock_response(payload):
    return Mock(json=Mock(return_value=payload))


def test_fetch_eu_country_metadata_returns_country_records(monkeypatch):
    payload = {
        "data": {
            "objects": [AUSTRIA],
            "meta": {"more": False, "total": 1, "count": 1, "limit": 100, "offset": 0},
        }
    }
    session = _mock_session_returning(_mock_response(payload))
    monkeypatch.setattr("weather_pipeline.country_clients.requests.Session", lambda: session)
    monkeypatch.setenv("REST_COUNTRIES_API_KEY", "test-key")

    assert fetch_eu_country_metadata() == [{
        "city_name": "Vienna",
        "country_code": "AT",
        "country_name": "Austria",
        "country_population": 9217573,
        "latitude": 48.2,
        "longitude": 16.37,
    }]


def test_fetch_eu_country_metadata_fetches_multiple_pages(monkeypatch):
    page_1 = {"data": {"objects": [AUSTRIA], "meta": {"more": True}}}
    page_2 = {"data": {"objects": [BELGIUM], "meta": {"more": False}}}
    session = _mock_session_returning(_mock_response(page_1), _mock_response(page_2))
    monkeypatch.setattr("weather_pipeline.country_clients.requests.Session", lambda: session)
    monkeypatch.setenv("REST_COUNTRIES_API_KEY", "test-key")

    result = fetch_eu_country_metadata()

    assert [c["country_code"] for c in result] == ["AT", "BE"]
    offsets = [call.kwargs["params"]["offset"] for call in session.get.call_args_list]
    assert offsets == [0, 1]


def test_fetch_eu_country_metadata_raises_on_http_error(monkeypatch):
    response = Mock()
    response.raise_for_status.side_effect = requests.HTTPError("Server error")
    session = _mock_session_returning(response)
    monkeypatch.setattr("weather_pipeline.country_clients.requests.Session", lambda: session)
    monkeypatch.setenv("REST_COUNTRIES_API_KEY", "test-key")

    with pytest.raises(requests.HTTPError):
        fetch_eu_country_metadata()

