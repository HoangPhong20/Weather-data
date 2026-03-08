from weather_pipeline.contracts import weather_schema
from weather_pipeline.transformations import validate_weather_schema


def test_validate_weather_schema_valid_payload():
    payload = {
        "name": "Hanoi",
        "dt": 1735600000,
        "sys": {"country": "VN"},
        "main": {"temp": 301.15, "humidity": 70},
        "wind": {"speed": 3.4},
    }

    assert validate_weather_schema(payload) is True


def test_validate_weather_schema_missing_nested_field():
    payload = {
        "name": "Hanoi",
        "dt": 1735600000,
        "sys": {},
        "main": {"temp": 301.15, "humidity": 70},
        "wind": {"speed": 3.4},
    }

    assert validate_weather_schema(payload) is False


def test_weather_schema_contract_contains_expected_fields():
    if weather_schema is None:
        return
    assert weather_schema.fieldNames() == ["name", "dt", "sys", "main", "wind"]
