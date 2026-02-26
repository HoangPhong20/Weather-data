import json

from extract import weather_api


def test_get_cities_from_env(monkeypatch):
    monkeypatch.setenv("WEATHER_CITIES", "Hanoi, Da Nang")

    cities = weather_api.get_cities()

    assert cities == ["Hanoi", "Da Nang"]


def test_get_cities_from_city_list_file(monkeypatch, tmp_path):
    city_file = tmp_path / "city.list.json"
    city_file.write_text(
        json.dumps(
            [
                {"name": "Ha Noi", "country": "VN"},
                {"name": "Da Nang", "country": "VN"},
                {"name": "Bangkok", "country": "TH"},
                {"name": "Ha Noi", "country": "VN"},
            ]
        ),
        encoding="utf-8",
    )

    monkeypatch.delenv("WEATHER_CITIES", raising=False)
    monkeypatch.setattr(weather_api, "CITY_LIST_FILE", city_file)
    monkeypatch.setattr(weather_api, "DEFAULT_COUNTRY_CODE", "VN")

    cities = weather_api.get_cities()

    assert cities == ["Da Nang", "Ha Noi"]


def test_get_cities_fallback_when_city_file_missing(monkeypatch, tmp_path):
    monkeypatch.delenv("WEATHER_CITIES", raising=False)
    monkeypatch.setattr(weather_api, "CITY_LIST_FILE", tmp_path / "missing.json")

    cities = weather_api.get_cities()

    assert cities == ["Hanoi"]
