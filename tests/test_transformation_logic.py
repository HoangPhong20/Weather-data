from weather_pipeline.transformations import (
    clean_weather,
    kelvin_to_celsius,
    normalize_country_code,
    parse_weather,
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


def test_parse_weather_dataframe(spark):
    raw_json = '{"name":"Hanoi","dt":1735600000,"sys":{"country":"vn"},"main":{"temp":300.15,"humidity":65},"wind":{"speed":2.1}}'
    df = spark.createDataFrame([(raw_json,)], ["raw_json"])

    parsed_df = parse_weather(df)
    row = parsed_df.collect()[0]

    assert row["city"] == "Hanoi"
    assert row["country"] == "VN"
    assert round(row["temperature"], 2) == 27.0
    assert row["humidity"] == 65
    assert row["wind_speed"] == 2.1


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
