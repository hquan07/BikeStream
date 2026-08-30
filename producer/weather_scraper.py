#!/usr/bin/env python3
"""
Weather Scraper — Fetches current weather data from OpenWeatherMap API
for each city and publishes to Kafka topic 'weather_events'.

Runs as a background thread inside the producer container,
polling every 15 minutes.
"""

import os
import time
import logging
from datetime import datetime, timezone

import requests
from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

logger = logging.getLogger("weather-scraper")

# City coordinates (approximate city centers)
CITY_COORDS = {
    "chicago":       {"lat": 41.8781, "lon": -87.6298},
    "new_york":      {"lat": 40.7128, "lon": -74.0060},
    "san_francisco": {"lat": 37.7749, "lon": -122.4194},
    "washington_dc": {"lat": 38.9072, "lon": -77.0369},
    "boston":         {"lat": 42.3601, "lon": -71.0589},
}

WEATHER_AVRO_SCHEMA = """{
  "type": "record",
  "name": "WeatherEvent",
  "namespace": "com.bikestream",
  "fields": [
    {"name": "city",            "type": "string"},
    {"name": "timestamp",       "type": "string"},
    {"name": "temp_celsius",    "type": "double"},
    {"name": "feels_like",      "type": "double"},
    {"name": "humidity",        "type": "int"},
    {"name": "pressure",        "type": "int"},
    {"name": "wind_speed",      "type": "double"},
    {"name": "weather_main",    "type": "string"},
    {"name": "weather_desc",    "type": "string"},
    {"name": "rain_1h_mm",      "type": "double"},
    {"name": "snow_1h_mm",      "type": "double"},
    {"name": "clouds_pct",      "type": "int"},
    {"name": "visibility_m",    "type": "int"}
  ]
}"""


def weather_to_dict(weather, ctx):
    return weather


def fetch_weather(city: str, coords: dict, api_key: str) -> dict:
    """Fetch current weather from OpenWeatherMap."""
    url = "https://api.openweathermap.org/data/2.5/weather"
    params = {
        "lat": coords["lat"],
        "lon": coords["lon"],
        "appid": api_key,
        "units": "metric",
    }
    resp = requests.get(url, params=params, timeout=15)
    resp.raise_for_status()
    data = resp.json()

    return {
        "city": city,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "temp_celsius": float(data["main"]["temp"]),
        "feels_like": float(data["main"]["feels_like"]),
        "humidity": int(data["main"]["humidity"]),
        "pressure": int(data["main"]["pressure"]),
        "wind_speed": float(data.get("wind", {}).get("speed", 0)),
        "weather_main": data["weather"][0]["main"] if data.get("weather") else "Unknown",
        "weather_desc": data["weather"][0]["description"] if data.get("weather") else "",
        "rain_1h_mm": float(data.get("rain", {}).get("1h", 0)),
        "snow_1h_mm": float(data.get("snow", {}).get("1h", 0)),
        "clouds_pct": int(data.get("clouds", {}).get("all", 0)),
        "visibility_m": int(data.get("visibility", 10000)),
    }


def start_weather_loop(producer: Producer, schema_registry_url: str):
    """Background loop that scrapes weather every 15 minutes."""
    api_key = os.getenv("OPENWEATHERMAP_API_KEY", "")
    if not api_key:
        logger.warning("OPENWEATHERMAP_API_KEY not set. Weather scraping disabled.")
        return

    topic = "weather_events"

    # Setup Avro serializer
    sr_client = SchemaRegistryClient({"url": schema_registry_url})
    avro_serializer = AvroSerializer(sr_client, WEATHER_AVRO_SCHEMA, weather_to_dict)

    logger.info("Weather scraper started (15-min interval)")

    while True:
        for city, coords in CITY_COORDS.items():
            try:
                weather = fetch_weather(city, coords, api_key)
                producer.produce(
                    topic=topic,
                    key=city,
                    value=avro_serializer(
                        weather,
                        SerializationContext(topic, MessageField.VALUE)
                    ),
                )
                logger.info(f"[Weather] {city}: {weather['temp_celsius']}°C, {weather['weather_main']}")
            except Exception as e:
                logger.error(f"[Weather] {city} fetch failed: {e}")

        producer.flush()
        time.sleep(900)  # 15 minutes
