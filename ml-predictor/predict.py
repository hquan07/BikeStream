#!/usr/bin/env python3
"""
ML Prediction Loop — Runs every 5 minutes, loads trained XGBoost models,
predicts fill_ratio 30 minutes into the future for each station,
and writes results to the 'station_predictions' table in TimescaleDB.

Also triggers model retraining every 24 hours.
"""

import os
import time
import logging
import pickle
import subprocess
from datetime import datetime, timezone, timedelta

import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# --- Logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("ml-predict")

# --- Config ---
PG_HOST     = os.getenv("POSTGRES_HOST", "timescaledb")
PG_PORT     = os.getenv("POSTGRES_PORT", "5432")
PG_USER     = os.getenv("POSTGRES_USER", "bikestream")
PG_PASS     = os.getenv("POSTGRES_PASSWORD", "bikestream_2024")
PG_DB       = os.getenv("POSTGRES_DB", "bikestream")
MODEL_DIR   = "/app/models"
PREDICT_INTERVAL = 300  # 5 minutes
RETRAIN_INTERVAL = 86400  # 24 hours


def get_conn():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT,
        user=PG_USER, password=PG_PASS,
        dbname=PG_DB
    )


def load_model(city: str):
    """Load a trained XGBoost model for a city."""
    model_path = os.path.join(MODEL_DIR, f"xgb_{city}.pkl")
    if not os.path.exists(model_path):
        return None
    with open(model_path, "rb") as f:
        return pickle.load(f)


def fetch_latest_data(city: str) -> pd.DataFrame:
    """Fetch the most recent snapshots for each station (last 30 min)."""
    query = """
        SELECT
            time, station_id, station_name, lat, lon,
            fill_ratio, num_bikes_available, num_docks_available, capacity
        FROM station_snapshots
        WHERE city = %s
          AND time > NOW() - INTERVAL '30 minutes'
          AND fill_ratio IS NOT NULL
        ORDER BY station_id, time;
    """
    conn = get_conn()
    try:
        return pd.read_sql(query, conn, params=(city,))
    finally:
        conn.close()


def engineer_predict_features(df: pd.DataFrame) -> pd.DataFrame:
    """Create features matching the training pipeline for prediction."""
    df = df.copy()
    df["time"] = pd.to_datetime(df["time"])
    df = df.sort_values(["station_id", "time"]).reset_index(drop=True)

    # Temporal features
    df["hour"] = df["time"].dt.hour
    df["dow"] = df["time"].dt.dayofweek
    df["is_weekend"] = (df["dow"] >= 5).astype(int)
    df["hour_sin"] = np.sin(2 * np.pi * df["hour"] / 24)
    df["hour_cos"] = np.cos(2 * np.pi * df["hour"] / 24)

    # Lag features
    for lag in [1, 2, 6, 12]:
        df[f"fill_lag_{lag}"] = df.groupby("station_id")["fill_ratio"].shift(lag)

    # Rolling average
    df["fill_rolling_mean_6"] = (
        df.groupby("station_id")["fill_ratio"]
        .transform(lambda x: x.rolling(6, min_periods=1).mean())
    )

    return df


def predict_and_write(city: str, model_data: dict):
    """Run prediction for a city and write to DB."""
    model = model_data["model"]
    feature_cols = model_data["features"]

    df = fetch_latest_data(city)
    if len(df) < 2:
        logger.warning(f"[{city}] Not enough recent data for prediction.")
        return

    df = engineer_predict_features(df)

    # Use the latest record per station for prediction
    latest = df.groupby("station_id").last().reset_index()
    latest = latest.dropna(subset=[c for c in feature_cols if c in latest.columns])

    if len(latest) == 0:
        logger.warning(f"[{city}] No valid features after engineering.")
        return

    X = latest[feature_cols]
    predicted_fill = model.predict(X)

    # Determine predicted status
    def classify_status(fill):
        if fill <= 0.0:
            return "PRED_EMPTY"
        elif fill < 0.2:
            return "PRED_LOW"
        elif fill > 0.9:
            return "PRED_HIGH"
        elif fill >= 1.0:
            return "PRED_FULL"
        else:
            return "PRED_HEALTHY"

    now = datetime.now(timezone.utc)
    prediction_target = now + timedelta(minutes=30)

    records = []
    for i, row in latest.iterrows():
        pred_fill = float(np.clip(predicted_fill[latest.index.get_loc(i)], 0, 1))
        records.append((
            now,
            prediction_target,
            city,
            row["station_id"],
            row.get("station_name", ""),
            float(row.get("lat", 0)),
            float(row.get("lon", 0)),
            row["fill_ratio"],
            pred_fill,
            classify_status(pred_fill),
            pred_fill <= 0.05 or pred_fill >= 0.95,  # needs_rebalancing_predicted
        ))

    # Write to DB
    conn = get_conn()
    try:
        cur = conn.cursor()
        execute_values(cur, """
            INSERT INTO station_predictions
            (prediction_time, target_time, city, station_id, station_name,
             lat, lon, current_fill_ratio, predicted_fill_ratio,
             predicted_status, needs_rebalancing_predicted)
            VALUES %s
        """, records)
        conn.commit()
        cur.close()
        logger.info(f"[{city}] Wrote {len(records)} predictions")
    except Exception as e:
        conn.rollback()
        logger.error(f"[{city}] Failed to write predictions: {e}")
    finally:
        conn.close()


def run_training():
    """Trigger training subprocess."""
    logger.info("Triggering model retraining...")
    try:
        subprocess.run(
            ["python", "-u", "train.py"],
            check=True, timeout=600
        )
        logger.info("Retraining complete.")
    except subprocess.TimeoutExpired:
        logger.error("Training timed out after 10 minutes.")
    except subprocess.CalledProcessError as e:
        logger.error(f"Training failed: {e}")


def main():
    logger.info("=== Starting ML Prediction Loop ===")

    # Initial training
    run_training()

    last_retrain = time.time()

    while True:
        loop_start = time.time()

        # Retrain every 24 hours
        if time.time() - last_retrain > RETRAIN_INTERVAL:
            run_training()
            last_retrain = time.time()

        # Discover cities from available models
        if not os.path.exists(MODEL_DIR):
            logger.warning("No models directory found. Waiting for training...")
            time.sleep(60)
            continue

        model_files = [f for f in os.listdir(MODEL_DIR) if f.endswith(".pkl")]
        if not model_files:
            logger.warning("No trained models found. Waiting...")
            time.sleep(60)
            continue

        for model_file in model_files:
            city = model_file.replace("xgb_", "").replace(".pkl", "")
            model_data = load_model(city)
            if model_data:
                try:
                    predict_and_write(city, model_data)
                except Exception as e:
                    logger.error(f"[{city}] Prediction failed: {e}")

        elapsed = time.time() - loop_start
        sleep_time = max(0, PREDICT_INTERVAL - elapsed)
        logger.info(f"Prediction cycle done in {elapsed:.1f}s. Next in {sleep_time:.0f}s.")
        time.sleep(sleep_time)


if __name__ == "__main__":
    main()
