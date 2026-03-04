import json
import logging
import os
from datetime import datetime, timezone

import requests


logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)

API_URL = os.getenv("WEATHER_API_URL", "https://api.openweathermap.org/data/2.5/weather")
CITIES = [city.strip() for city in os.getenv("WEATHER_CITIES", "Hanoi").split(",") if city.strip()]
OUTPUT_FILE = os.getenv("WEATHER_OUTPUT_FILE", "data/weather_raw.jsonl")


def get_api_key() -> str:
    api_key = os.getenv("OPENWEATHER_API_KEY")
    if not api_key:
        raise ValueError("OPENWEATHER_API_KEY is required")
    return api_key


def fetch_city_weather(city: str) -> dict:
    logger.info("Fetching weather for city=%s", city)
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
    output_dir = os.path.dirname(OUTPUT_FILE)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    logger.info("Writing weather data to %s", OUTPUT_FILE)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as out:
        for city in CITIES:
            record = fetch_city_weather(city)
            out.write(json.dumps(record, ensure_ascii=False) + "\n")

    logger.info("Completed weather extraction for %d city(s)", len(CITIES))


if __name__ == "__main__":
    run()
