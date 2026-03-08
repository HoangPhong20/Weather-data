from typing import Any

try:
    from pyspark.sql import DataFrame
    from pyspark.sql.functions import col
except ModuleNotFoundError:  # pragma: no cover
    DataFrame = Any


def is_temperature_in_expected_range(temp_celsius: float) -> bool:
    return -90.0 <= temp_celsius <= 60.0


def has_valid_null_rate(total_rows: int, null_rows: int, threshold: float = 0.05) -> bool:
    if total_rows <= 0:
        return False
    return (null_rows / total_rows) <= threshold


def is_country_code_valid(country: str) -> bool:
    return isinstance(country, str) and len(country) == 2 and country.isalpha() and country.upper() == country


def filter_valid_temperature(df: DataFrame) -> DataFrame:
    if "col" not in globals():
        raise ModuleNotFoundError("pyspark is required to filter DataFrames")
    return df.filter(col("temperature").between(-80, 70))
