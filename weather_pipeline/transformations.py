from __future__ import annotations

from datetime import datetime, timezone
import logging

from typing import Any

try:
    from pyspark.sql import DataFrame
    from pyspark.sql.functions import col, from_json, from_unixtime, upper
except ModuleNotFoundError:  # pragma: no cover
    DataFrame = Any

from weather_pipeline.contracts import (
    REQUIRED_MAIN_FIELDS,
    REQUIRED_RAW_FIELDS,
    REQUIRED_SYS_FIELDS,
    REQUIRED_WIND_FIELDS,
    weather_schema,
)


logger = logging.getLogger(__name__)


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
        logger.warning("Invalid weather payload schema. payload_keys=%s", list(payload.keys()) if isinstance(payload, dict) else type(payload))
        raise ValueError("Invalid weather payload schema")

    return {
        "city": payload["name"],
        "country": normalize_country_code(payload["sys"]["country"]),
        "event_time": to_event_time(payload["dt"]),
        "temperature": kelvin_to_celsius(payload["main"]["temp"]),
        "humidity": int(payload["main"]["humidity"]),
        "wind_speed": float(payload["wind"]["speed"]),
    }


def parse_weather(df: DataFrame) -> DataFrame:
    if weather_schema is None:
        raise ModuleNotFoundError("pyspark is required to parse weather DataFrame")

    return (
        df.select(from_json(col("raw_json"), weather_schema).alias("w"))
        .select("w.*")
        .select(
            col("name").alias("city"),
            upper(col("sys.country")).alias("country"),
            from_unixtime(col("dt")).cast("timestamp").alias("event_time"),
            (col("main.temp") - 273.15).alias("temperature"),
            col("main.humidity").alias("humidity"),
            col("wind.speed").alias("wind_speed"),
        )
    )


def clean_weather(df: DataFrame) -> DataFrame:
    return df.dropna(subset=["city", "country", "event_time"]).dropDuplicates(["city", "country", "event_time"])
