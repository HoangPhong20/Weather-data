import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import List

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def resolve_project_path(raw_path: str) -> Path:
    candidate = Path(raw_path)
    if candidate.is_absolute():
        return candidate
    return (PROJECT_ROOT / raw_path.lstrip("/")).resolve()


API_URL = os.getenv("WEATHER_API_URL", "https://api.openweathermap.org/data/2.5/weather")
OUTPUT_FILE = resolve_project_path(os.getenv("WEATHER_OUTPUT_FILE", "data/weather_raw.jsonl"))
CITY_LIST_FILE = resolve_project_path(os.getenv("WEATHER_CITY_LIST_FILE", "/get_cities/city.list.json"))
DEFAULT_COUNTRY_CODE = os.getenv("WEATHER_COUNTRY_CODE", "VN")

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)


def get_api_key() -> str:
    api_key = os.getenv("OPENWEATHER_API_KEY")
    if not api_key:
        raise ValueError("OPENWEATHER_API_KEY is required")
    return api_key


def get_cities() -> List[str]:
    configured = [city.strip() for city in os.getenv("WEATHER_CITIES", "").split(",") if city.strip()]
    if configured:
        return configured
    if not CITY_LIST_FILE.is_file():
        logger.warning("City list file not found: %s. Fallback to Hanoi.", CITY_LIST_FILE)
        return ["Hanoi"]
    try:
        with CITY_LIST_FILE.open("r", encoding="utf-8") as city_file:
            payload = json.load(city_file)
    except json.JSONDecodeError as exc:
        logger.error("Failed to parse city list (%s): %s. Fallback to Hanoi.", CITY_LIST_FILE, exc)
        return ["Hanoi"]
    filtered = sorted(
        {
            entry.get("name", "").strip()
            for entry in payload
            if entry.get("country") == DEFAULT_COUNTRY_CODE and entry.get("name")
        }
    )
    return filtered or ["Hanoi"]


def fetch_city_weather(city: str, api_key: str) -> dict:
    logger.info("Fetching weather data for city=%s", city)
    response = requests.get(
        API_URL,
        params={"q": city, "appid": api_key},
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    logger.debug("Fetched response payload keys for city=%s: %s", city, list(payload.keys()))
    return {
        "raw_json": json.dumps(payload, ensure_ascii=False),
        "ingestion_time": datetime.now(timezone.utc).isoformat(),
    }


def run() -> None:
    cities = get_cities()
    api_key = get_api_key()
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_FILE.open("w", encoding="utf-8") as out:
        logger.info("Writing weather records to %s for %d city(ies)", OUTPUT_FILE, len(cities))
        for city in cities:
            record = fetch_city_weather(city, api_key)
            out.write(json.dumps(record, ensure_ascii=False) + "\n")
    logger.info("Weather extraction completed successfully")


if __name__ == "__main__":
    run()
