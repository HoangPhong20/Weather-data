from weather_pipeline.transformations import (
    kelvin_to_celsius,
    normalize_country_code,
    to_event_time,
    transform_raw_weather,
)


def test_kelvin_to_celsius_conversion():
    assert kelvin_to_celsius(300.15) == 27.0


def test_normalize_country_code():
    assert normalize_country_code(" vn ") == "VN"


def test_transform_raw_weather_happy_path():
    payload = {
        "name": "Hanoi",
        "dt": 1735600000,
        "sys": {"country": "vn"},
        "main": {"temp": 300.15, "humidity": 65},
        "wind": {"speed": 2.1},
    }

    transformed = transform_raw_weather(payload)

    assert transformed["city"] == "Hanoi"
    assert transformed["country"] == "VN"
    assert transformed["temperature"] == 27.0
    assert transformed["humidity"] == 65
    assert transformed["wind_speed"] == 2.1
    assert transformed["event_time"] == to_event_time(1735600000)
