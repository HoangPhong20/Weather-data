from dataclasses import dataclass
from datetime import datetime

try:
    from pyspark.sql.types import (
        DoubleType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
    )
except ModuleNotFoundError:  # pragma: no cover
    StructType = None


@dataclass(frozen=True)
class SilverWeatherRecord:
    city: str
    country: str
    event_time: datetime
    temperature: float
    humidity: int
    wind_speed: float


REQUIRED_RAW_FIELDS = ("name", "dt", "sys", "main", "wind")
REQUIRED_SYS_FIELDS = ("country",)
REQUIRED_MAIN_FIELDS = ("temp", "humidity")
REQUIRED_WIND_FIELDS = ("speed",)

if StructType is not None:
    weather_schema = StructType(
        [
            StructField("name", StringType()),
            StructField("dt", LongType()),
            StructField("sys", StructType([StructField("country", StringType())])),
            StructField(
                "main",
                StructType(
                    [
                        StructField("temp", DoubleType()),
                        StructField("humidity", IntegerType()),
                    ]
                ),
            ),
            StructField("wind", StructType([StructField("speed", DoubleType())])),
        ]
    )
else:  # pragma: no cover
    weather_schema = None

__all__ = [
    "SilverWeatherRecord",
    "weather_schema",
]
