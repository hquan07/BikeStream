#!/usr/bin/env python3
"""
GBFS Producer — Polls 5 cities' bike-share APIs and publishes to Kafka.

Each message on topic 'station_status' is a JSON object with:
  city, station_id, timestamp, num_bikes_available, num_docks_available,
  num_ebikes_available, is_renting, is_returning, station_name, lat, lon, capacity
"""

import os
import json
import time
import logging
from datetime import datetime, timezone

import requests
import yaml
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# --- Logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("gbfs-producer")

# --- Load Config ---
def load_config(path="config.yaml"):
    with open(path) as f:
        raw = f.read()
    # Substitute env vars
    for key, val in os.environ.items():
        raw = raw.replace(f"${{{key}}}", val)
        raw = raw.replace(f"${{{key}:-{val}}}", val)
    return yaml.safe_load(raw)

# --- Fetch station info (cached — changes rarely) ---
def fetch_station_info(url: str) -> dict:
    """Returns dict mapping station_id -> {name, lat, lon, capacity}"""
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        stations = data.get("data", {}).get("stations", [])
        lookup = {}
        for s in stations:
            sid = s.get("station_id", "")
            lookup[sid] = {
                "station_name": s.get("name", "Unknown"),
                "lat": s.get("lat", 0.0),
                "lon": s.get("lon", 0.0),
                "capacity": s.get("capacity", 0),
            }
        return lookup
    except Exception as e:
        logger.error(f"Failed to fetch station info from {url}: {e}")
        return {}

# --- Fetch station status and produce to Kafka ---
def poll_and_produce(producer: KafkaProducer, city: dict, info_cache: dict, topic: str):
    """Poll one city's station_status and produce each station as a Kafka message."""
    try:
        resp = requests.get(city["status_url"], timeout=30)
        resp.raise_for_status()
        data = resp.json()
        stations = data.get("data", {}).get("stations", [])
        now = datetime.now(timezone.utc).isoformat()
        count = 0

        for s in stations:
            sid = s.get("station_id", "")
            info = info_cache.get(sid, {})

            message = {
                "city": city["name"],
                "system": city["system"],
                "station_id": sid,
                "timestamp": now,
                "num_bikes_available": s.get("num_bikes_available", 0),
                "num_docks_available": s.get("num_docks_available", 0),
                "num_ebikes_available": s.get("num_ebikes_available", 0),
                "num_scooters_available": s.get("num_scooters_available", 0),
                "is_renting": bool(s.get("is_renting", 0)),
                "is_returning": bool(s.get("is_returning", 0)),
                "station_name": info.get("station_name", "Unknown"),
                "lat": info.get("lat", 0.0),
                "lon": info.get("lon", 0.0),
                "capacity": info.get("capacity", 0),
            }

            # Key = city:station_id for partition affinity
            key = f"{city['name']}:{sid}".encode("utf-8")
            producer.send(topic, key=key, value=message)
            count += 1

        producer.flush()
        logger.info(f"[{city['name']}] Produced {count} station records")

    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 429:
            logger.warning(f"[{city['name']}] Rate limited (429)! Triggering backoff.")
            raise e
        else:
            logger.error(f"[{city['name']}] HTTP Error: {e}")
    except Exception as e:
        logger.error(f"[{city['name']}] Poll failed: {e}")

# --- Main Loop ---
def main():
    config = load_config()
    broker = config["kafka"]["bootstrap_servers"]
    topic = config["kafka"]["topic"]
    interval = config["poll_interval_seconds"]

    # Wait for Kafka to be ready
    producer = None
    for attempt in range(30):
        try:
            producer = KafkaProducer(
                bootstrap_servers=broker,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=3,
            )
            logger.info(f"Connected to Kafka at {broker}")
            break
        except NoBrokersAvailable:
            logger.warning(f"Kafka not ready (attempt {attempt+1}/30), retrying in 5s...")
            time.sleep(5)

    if producer is None:
        logger.critical("Could not connect to Kafka after 30 attempts. Exiting.")
        return

    # Pre-fetch station info for all cities (refresh every 30 minutes)
    info_caches = {}
    last_info_refresh = 0

    logger.info(f"Starting producer loop: {len(config['cities'])} cities, every {interval}s")

    backoff_time = 0
    while True:
        if backoff_time > 0:
            logger.info(f"Backing off for {backoff_time}s...")
            time.sleep(backoff_time)
            backoff_time = 0

        loop_start = time.time()

        # Refresh station info every 30 minutes
        if time.time() - last_info_refresh > 1800:
            for city in config["cities"]:
                logger.info(f"[{city['name']}] Refreshing station info...")
                info_caches[city["name"]] = fetch_station_info(city["info_url"])
                logger.info(f"[{city['name']}] Cached {len(info_caches[city['name']])} stations")
            last_info_refresh = time.time()

        # Poll all cities
        try:
            for city in config["cities"]:
                poll_and_produce(producer, city, info_caches.get(city["name"], {}), topic)
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                backoff_time = 60 # wait 1 min if rate limited
                continue

        # Sleep for remaining interval
        elapsed = time.time() - loop_start
        sleep_time = max(0, interval - elapsed)
        logger.info(f"Cycle complete in {elapsed:.1f}s. Sleeping {sleep_time:.1f}s...")
        time.sleep(sleep_time)


if __name__ == "__main__":
    main()
