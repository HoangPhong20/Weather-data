from weather_pipeline.business_rules import (
    has_valid_null_rate,
    is_country_code_valid,
    is_temperature_in_expected_range,
)


def test_temperature_business_rule():
    assert is_temperature_in_expected_range(35.0) is True
    assert is_temperature_in_expected_range(-120.0) is False


def test_null_rate_business_rule():
    assert has_valid_null_rate(total_rows=100, null_rows=4, threshold=0.05) is True
    assert has_valid_null_rate(total_rows=100, null_rows=8, threshold=0.05) is False


def test_country_code_business_rule():
    assert is_country_code_valid("VN") is True
    assert is_country_code_valid("VNM") is False
    assert is_country_code_valid("vn") is False
