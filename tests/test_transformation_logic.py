import pytest

from weather_pipeline.transformations import (
    clean_weather,
    kelvin_to_celsius,
    normalize_country_code,
    parse_weather,
    to_event_time,
    transform_raw_weather,
)

# --------------------------------------------------
# Unit tests (pure python functions)
# --------------------------------------------------

@pytest.mark.parametrize(
    "kelvin, expected",
    [
        (300.15, 27.0),
        (273.15, 0.0),
    ],
)
def test_kelvin_to_celsius_conversion(kelvin, expected):
    assert kelvin_to_celsius(kelvin) == expected
# pytest.mark.parametrize
# @pytest.mark.parametrize("x", [1,2,3])
# def test(x):
#     ...

# test = parametrize(...)(test)

# Decorator này nói với pytest:

# hãy chạy test nhiều lần với data khác nhau

@pytest.mark.parametrize(
    "raw, normalized",
    [
        (" vn ", "VN"),
        ("us", "US"),
        (" JP", "JP"),
    ],
)
def test_normalize_country_code(raw, normalized):
    assert normalize_country_code(raw) == normalized


# --------------------------------------------------
# Transformation contract test
# --------------------------------------------------

VALID_PAYLOAD = {
    "name": "Hanoi",
    "dt": 1735600000,
    "sys": {"country": "vn"},
    "main": {"temp": 300.15, "humidity": 65},
    "wind": {"speed": 2.1},
}


def test_transform_raw_weather_happy_path():
    transformed = transform_raw_weather(VALID_PAYLOAD)

    assert transformed == {
        "city": "Hanoi",
        "country": "VN",
        "event_time": to_event_time(1735600000),
        "temperature": 27.0,
        "humidity": 65,
        "wind_speed": 2.1,
    }


# --------------------------------------------------
# Spark dataframe tests
# --------------------------------------------------

def test_parse_weather_dataframe(spark):
    raw_json = """
    {"name":"Hanoi","dt":1735600000,
     "sys":{"country":"vn"},
     "main":{"temp":300.15,"humidity":65},
     "wind":{"speed":2.1}}
    """

    df = spark.createDataFrame([(raw_json,)], ["raw_json"])

    result = parse_weather(df).collect()[0]

    assert result.asDict() == {
        "city": "Hanoi",
        "country": "VN",
        "event_time": result["event_time"],  # timestamp validated implicitly
        "temperature": 27.0,
        "humidity": 65,
        "wind_speed": 2.1,
    }


def test_clean_weather_deduplicate_and_dropna(spark):
    df = spark.createDataFrame(
        [
            ("Hanoi", "VN", "2025-01-01 00:00:00", 27.0, 65, 2.1),
            ("Hanoi", "VN", "2025-01-01 00:00:00", 27.0, 65, 2.1),
            (None, "VN", "2025-01-01 00:00:00", 27.0, 65, 2.1),
        ],
        ["city", "country", "event_time", "temperature", "humidity", "wind_speed"],
    )

    cleaned = clean_weather(df)

    assert cleaned.count() == 1