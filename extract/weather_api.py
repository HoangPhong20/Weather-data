import json
import os
from datetime import datetime, timezone
from pathlib import Path



API_URL = os.getenv("WEATHER_API_URL", "https://api.openweathermap.org/data/2.5/weather")
OUTPUT_FILE = os.getenv("WEATHER_OUTPUT_FILE", "data/weather_raw.jsonl")
CITY_LIST_FILE = Path(os.getenv("WEATHER_CITY_LIST_FILE", "ETL/extract/get_cities/city.list.json"))
DEFAULT_COUNTRY_CODE = os.getenv("WEATHER_COUNTRY_CODE", "VN")


def get_cities() -> list[str]:
    configured_cities = [city.strip() for city in os.getenv("WEATHER_CITIES", "").split(",") if city.strip()]
    if configured_cities:
        return configured_cities

    if not CITY_LIST_FILE.exists():
        return ["Hanoi"]

    with CITY_LIST_FILE.open("r", encoding="utf-8") as city_file:
        cities = json.load(city_file)

    filtered_cities = sorted(
        {
            city["name"]
            for city in cities
            if city.get("country") == DEFAULT_COUNTRY_CODE and city.get("name")
        }
    )
    return filtered_cities or ["Hanoi"]


def get_api_key() -> str:
    api_key = os.getenv("OPENWEATHER_API_KEY")
    if not api_key:
        raise ValueError("OPENWEATHER_API_KEY is required")
    return api_key


def fetch_city_weather(city: str) -> dict:
    import requests

    response = requests.get(
        API_URL,
        params={"q": city, "appid": get_api_key()},
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    return {
        "raw_json": json.dumps(payload, ensure_ascii=False),
        "ingestion_time": datetime.now(timezone.utc).isoformat(),
    }


def run() -> None:
    cities = get_cities()
    output_dir = os.path.dirname(OUTPUT_FILE)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as out:
        for city in cities:
            record = fetch_city_weather(city)
            out.write(json.dumps(record, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    run()
