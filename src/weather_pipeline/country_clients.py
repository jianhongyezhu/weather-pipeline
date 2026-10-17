import os

import requests

API_URL = "https://api.restcountries.com/countries/v5"

def fetch_eu_country_metadata(api_key=None):
    """Fetch metadata and capital coordinates for EU countries."""
    api_key = api_key or os.getenv("REST_COUNTRIES_API_KEY")

    if not api_key:
        raise ValueError("REST_COUNTRIES_API_KEY is not set")

    headers = {"Authorization": f"Bearer {api_key}"}
    params = {
        "region": "Europe",  
        "memberships.eu": "1",
        "limit": 100,
        "offset": 0,
    }

    countries = []

    with requests.Session() as session:
        while True:
            response = session.get(
                API_URL,
                headers=headers,
                params=params,
                timeout=20,
            )
            response.raise_for_status()
            payload = response.json()

            data = payload["data"]

            for country in data["objects"]:
                capitals = country.get("capitals", [])

                # Prefer the primary capital with valid coordinates.
                capital = next(
                    (
                        c for c in capitals
                        if c.get("attributes", {}).get("primary")
                        and c.get("coordinates", {}).get("lat") is not None
                        and c.get("coordinates", {}).get("lng") is not None
                    ),
                    None,
                )

                if capital is None:
                    continue

                countries.append({
                    "city_name": capital["name"],
                    "country_code": country["codes"]["alpha_2"],
                    "country_name": country["names"]["common"],
                    "country_population": country["population"],
                    "latitude": capital["coordinates"]["lat"],
                    "longitude": capital["coordinates"]["lng"],
                })

            if not data["meta"]["more"]:
                break
            # Update the offset for the next page of results.
            params = {**params, "offset": params["offset"] + len(data["objects"])}

    return sorted(countries, key=lambda c: c["country_code"])
