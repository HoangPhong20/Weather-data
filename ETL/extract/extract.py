import os
import json
import time
import logging
import requests
from kafka import KafkaProducer
from concurrent.futures import ThreadPoolExecutor, as_completed


# -------------------------------------------------------------------
# LOGGING
# -------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("weather-producer")


# -------------------------------------------------------------------
# CONFIG
# -------------------------------------------------------------------

API_KEY = os.getenv("API_KEY")
API_URL = os.getenv("API_URL")
JSON_PATH = os.getenv("json_path")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

if not API_KEY:
    raise ValueError("Missing API_KEY")

if not API_URL:
    raise ValueError("Missing API_URL")

if not JSON_PATH:
    raise ValueError("Missing json_path")


# -------------------------------------------------------------------
# LOAD CITY LIST
# -------------------------------------------------------------------

with open(JSON_PATH, "r", encoding="utf-8") as f:
    CITY_DATA = json.load(f)


def get_cities(country):
    return [c["name"] for c in CITY_DATA if c["country"] == country]

# -------------------------------------------------------------------
# KAFKA
# -------------------------------------------------------------------

def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode(),
        retries=5,
        linger_ms=100,
        batch_size=65536,
        acks=1,
    )

# -------------------------------------------------------------------
# API CALL
# -------------------------------------------------------------------

def fetch_weather(city, country, retries=3):
    params = {
        "q": f"{city},{country}",
        "appid": API_KEY,
        "lang": "en"
    }
    for attempt in range(retries):
        try:
            r = requests.get(API_URL, params=params, timeout=10)

            if r.status_code == 429:
                wait = 2 ** attempt
                logger.warning(f"Rate limit → sleep {wait}s")
                time.sleep(wait)
                continue

            r.raise_for_status()
            return r.json()

        except Exception as e:
            logger.warning(f"{city} retry {attempt+1}: {e}")
            time.sleep(2 ** attempt)

    logger.error(f"Failed after retries: {city}")
    return None

def send_weather(producer, city, country, topic):
    data = fetch_weather(city, country)
    if data:
        producer.send(topic, value=data)

def process_region(producer, countries, topic, workers=10):
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = []

        for code in countries:
            cities = get_cities(code)
            logger.info(f"{code}: {len(cities)} cities")

            for city in cities:
                futures.append(
                    executor.submit(send_weather, producer, city, code, topic)
                )

        for f in as_completed(futures):
            f.result()

    logger.info(f"Finished topic {topic}")

def run():
    producer = create_producer()
    regions = {
        "southeast_asia": {"VN", "TH", "MY", "SG", "ID", "PH", "KH", "LA"},
        "east_asia": {"JP", "KR", "CN", "TW", "HK", "MO"},
    }

    with ThreadPoolExecutor(max_workers=len(regions)) as executor:
        tasks = [
            executor.submit(process_region, producer, codes, topic)
            for topic, codes in regions.items()
        ]
        for t in as_completed(tasks):
            t.result()
    producer.flush()
    producer.close()
    logger.info("All weather data sent")

if __name__ == "__main__":
    run()