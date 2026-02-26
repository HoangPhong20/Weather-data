from dataclasses import dataclass


@dataclass(frozen=True)
class SilverWeatherRecord:
    city: str
    country: str
    event_time: str
    temperature: float
    humidity: int
    wind_speed: float


REQUIRED_RAW_FIELDS = ("name", "dt", "sys", "main", "wind")
REQUIRED_SYS_FIELDS = ("country",)
REQUIRED_MAIN_FIELDS = ("temp", "humidity")
REQUIRED_WIND_FIELDS = ("speed",)
