import pytest

from weather_pipeline.business_rules import (
    filter_valid_temperature,
    has_valid_null_rate,
    is_country_code_valid,
    is_temperature_in_expected_range,
)


# ======================================================
# Temperature rules
# ======================================================
# Test cases for temperature business rule with edge cases at -90 and 60 degrees Celsius
@pytest.mark.parametrize(
    "temperature,expected",
    [
        (35.0, True),
        (-90.0, True),     # lower boundary
        (60.0, True),      # upper boundary
        (-120.0, False),
        (61.0, False),
    ],
)
def test_temperature_business_rule(temperature, expected):
    assert is_temperature_in_expected_range(temperature) is expected


# ======================================================
# Null rate rules
# ======================================================

@pytest.mark.parametrize(
    "total,nulls,threshold,expected",
    [
        (100, 4, 0.05, True),
        (100, 8, 0.05, False),
        (0, 0, 0.05, False),  # edge case
    ],
)
def test_null_rate_business_rule(total, nulls, threshold, expected):
    assert has_valid_null_rate(total, nulls, threshold) is expected


# ======================================================
# Country code validation
# ======================================================

@pytest.mark.parametrize(
    "country,expected",
    [
        ("VN", True),
        ("VNM", False),
        ("vn", False),
        ("", False),
        (None, False),
        ("1N", False),
    ],
)
def test_country_code_business_rule(country, expected):
    assert is_country_code_valid(country) is expected


# ======================================================
# Spark DataFrame rule
# ======================================================

def test_filter_valid_temperature_dataframe(spark):
    df = spark.createDataFrame(
        [
            ("Hanoi", 27.0),
            ("Death Valley", 71.0),
            ("Antarctica", -85.0),
        ],
        ["city", "temperature"],
    )

    result = filter_valid_temperature(df)

    rows = {row["city"] for row in result.collect()}

    assert rows == {"Hanoi"}