from __future__ import annotations

from datetime import datetime, timezone

from weather_pipeline.contracts import (
    REQUIRED_MAIN_FIELDS,
    REQUIRED_RAW_FIELDS,
    REQUIRED_SYS_FIELDS,
    REQUIRED_WIND_FIELDS,
)


def validate_weather_schema(payload: dict) -> bool:
    if not isinstance(payload, dict):
        return False

    if any(field not in payload for field in REQUIRED_RAW_FIELDS):
        return False

    if not isinstance(payload.get("sys"), dict) or any(k not in payload["sys"] for k in REQUIRED_SYS_FIELDS):
        return False

    if not isinstance(payload.get("main"), dict) or any(k not in payload["main"] for k in REQUIRED_MAIN_FIELDS):
        return False

    if not isinstance(payload.get("wind"), dict) or any(k not in payload["wind"] for k in REQUIRED_WIND_FIELDS):
        return False

    return True


def kelvin_to_celsius(kelvin: float) -> float:
    return round(kelvin - 273.15, 2)


def normalize_country_code(country: str) -> str:
    return (country or "").strip().upper()


def to_event_time(unix_ts: int) -> str:
    return datetime.fromtimestamp(unix_ts, tz=timezone.utc).isoformat()


def transform_raw_weather(payload: dict) -> dict:
    if not validate_weather_schema(payload):
        raise ValueError("Invalid weather payload schema")

    return {
        "city": payload["name"],
        "country": normalize_country_code(payload["sys"]["country"]),
        "event_time": to_event_time(payload["dt"]),
        "temperature": kelvin_to_celsius(payload["main"]["temp"]),
        "humidity": int(payload["main"]["humidity"]),
        "wind_speed": float(payload["wind"]["speed"]),
    }
