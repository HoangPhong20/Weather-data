import pytest

from weather_pipeline.contracts import weather_schema
from weather_pipeline.transformations import validate_weather_schema


# --------------------------------------------------
# Test data
# --------------------------------------------------

VALID_PAYLOAD = {
    "name": "Hanoi",
    "dt": 1735600000,
    "sys": {"country": "VN"},
    "main": {"temp": 301.15, "humidity": 70},
    "wind": {"speed": 3.4},
}


# --------------------------------------------------
# Schema validation tests
# --------------------------------------------------

@pytest.mark.parametrize(
    "payload, expected",
    [
        (VALID_PAYLOAD, True),
        (
            {
                **VALID_PAYLOAD,
                "sys": {},  # missing nested field
            },
            False,
        ),
        ({}, False),
        (None, False),
    ],
)
def test_validate_weather_schema(payload, expected):
    assert validate_weather_schema(payload) is expected


# --------------------------------------------------
# Contract tests (schema structure)
# --------------------------------------------------

def test_weather_schema_contract_contains_expected_fields():
    if weather_schema is None:
        pytest.skip("pyspark not installed")

    assert weather_schema.fieldNames() == [
        "name",
        "dt",
        "sys",
        "main",
        "wind",
    ]