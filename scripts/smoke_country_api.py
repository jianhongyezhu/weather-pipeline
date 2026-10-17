from weather_pipeline.country_clients import fetch_eu_country_metadata

countries = fetch_eu_country_metadata()

print(f"Number of countries: {len(countries)}")
print("First 100 records:")

for country in countries[:100]:
    print(country)
