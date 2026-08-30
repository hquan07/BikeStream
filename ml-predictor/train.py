#!/usr/bin/env python3
"""
ML Model Training Script — XGBoost Regressor for station fill ratio prediction.

Queries historical station_snapshots from TimescaleDB, engineers temporal features
(hour-of-day, day-of-week, rolling averages), and trains a per-city XGBoost model
to predict fill_ratio 30 minutes into the future.

Models are saved to /app/models/ for the prediction loop to consume.
"""

import os
import time
import logging
import pickle
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import psycopg2
from xgboost import XGBRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, mean_absolute_error

# --- Logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("ml-train")

# --- Config ---
PG_HOST     = os.getenv("POSTGRES_HOST", "timescaledb")
PG_PORT     = os.getenv("POSTGRES_PORT", "5432")
PG_USER     = os.getenv("POSTGRES_USER", "bikestream")
PG_PASS     = os.getenv("POSTGRES_PASSWORD", "bikestream_2024")
PG_DB       = os.getenv("POSTGRES_DB", "bikestream")
MODEL_DIR   = "/app/models"


def get_conn():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT,
        user=PG_USER, password=PG_PASS,
        dbname=PG_DB
    )


def fetch_training_data(city: str, lookback_hours: int = 72) -> pd.DataFrame:
    """Fetch historical station snapshots for a given city."""
    query = f"""
        SELECT
            time, station_id, fill_ratio,
            num_bikes_available, num_docks_available, capacity
        FROM station_snapshots
        WHERE city = %s
          AND time > NOW() - INTERVAL '{lookback_hours} hours'
          AND fill_ratio IS NOT NULL
        ORDER BY station_id, time;
    """
    conn = get_conn()
    try:
        df = pd.read_sql(query, conn, params=(city,))
        return df
    finally:
        conn.close()


def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    """Create temporal and lag features for prediction."""
    df = df.copy()
    df["time"] = pd.to_datetime(df["time"])
    df = df.sort_values(["station_id", "time"]).reset_index(drop=True)

    # Temporal features
    df["hour"] = df["time"].dt.hour
    df["dow"] = df["time"].dt.dayofweek  # 0=Monday
    df["is_weekend"] = (df["dow"] >= 5).astype(int)
    df["hour_sin"] = np.sin(2 * np.pi * df["hour"] / 24)
    df["hour_cos"] = np.cos(2 * np.pi * df["hour"] / 24)

    # Lag features (per station): fill_ratio at t-1, t-2, t-6 (30min intervals ~ 1, 2, 6 snapshots)
    for lag in [1, 2, 6, 12]:
        df[f"fill_lag_{lag}"] = df.groupby("station_id")["fill_ratio"].shift(lag)

    # Rolling average (6 snapshots ~ 3 min at 30s interval)
    df["fill_rolling_mean_6"] = (
        df.groupby("station_id")["fill_ratio"]
        .transform(lambda x: x.rolling(6, min_periods=1).mean())
    )

    # Target: fill_ratio 30 min (approx 60 snapshots) in the future
    df["target_fill_30m"] = df.groupby("station_id")["fill_ratio"].shift(-60)

    # Drop rows with NaN target/features
    df = df.dropna(subset=[
        "target_fill_30m", "fill_lag_1", "fill_lag_2",
        "fill_lag_6", "fill_lag_12"
    ])

    return df


def train_city_model(city: str) -> dict:
    """Train XGBoost model for a specific city."""
    logger.info(f"[{city}] Fetching training data...")
    df = fetch_training_data(city)

    if len(df) < 100:
        logger.warning(f"[{city}] Not enough data ({len(df)} rows). Skipping.")
        return None

    logger.info(f"[{city}] Engineering features from {len(df)} rows...")
    df = engineer_features(df)

    if len(df) < 50:
        logger.warning(f"[{city}] Not enough features after engineering. Skipping.")
        return None

    feature_cols = [
        "hour", "dow", "is_weekend", "hour_sin", "hour_cos",
        "fill_ratio", "num_bikes_available", "num_docks_available", "capacity",
        "fill_lag_1", "fill_lag_2", "fill_lag_6", "fill_lag_12",
        "fill_rolling_mean_6"
    ]

    X = df[feature_cols]
    y = df["target_fill_30m"]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, shuffle=False  # Time-series: no shuffle
    )

    model = XGBRegressor(
        n_estimators=200,
        max_depth=6,
        learning_rate=0.1,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=42,
        n_jobs=-1,
    )

    logger.info(f"[{city}] Training XGBoost on {len(X_train)} samples...")
    model.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=False)

    y_pred = model.predict(X_test)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    mae = mean_absolute_error(y_test, y_pred)

    logger.info(f"[{city}] Model metrics — RMSE: {rmse:.4f}, MAE: {mae:.4f}")

    # Save model
    os.makedirs(MODEL_DIR, exist_ok=True)
    model_path = os.path.join(MODEL_DIR, f"xgb_{city}.pkl")
    with open(model_path, "wb") as f:
        pickle.dump({"model": model, "features": feature_cols}, f)

    logger.info(f"[{city}] Model saved to {model_path}")
    return {"city": city, "rmse": rmse, "mae": mae}


def main():
    """Train models for all cities."""
    logger.info("=== Starting ML Training Pipeline ===")

    # Wait for DB to have some data
    conn = None
    for attempt in range(30):
        try:
            conn = get_conn()
            cur = conn.cursor()
            cur.execute("SELECT DISTINCT city FROM station_snapshots LIMIT 10")
            cities = [row[0] for row in cur.fetchall()]
            cur.close()
            conn.close()
            if cities:
                break
        except Exception as e:
            logger.warning(f"DB not ready (attempt {attempt+1}/30): {e}")
            time.sleep(10)

    if not cities:
        logger.error("No cities found in station_snapshots. Exiting.")
        return

    logger.info(f"Found {len(cities)} cities: {cities}")

    results = []
    for city in cities:
        result = train_city_model(city)
        if result:
            results.append(result)

    logger.info("=== Training Complete ===")
    for r in results:
        logger.info(f"  {r['city']}: RMSE={r['rmse']:.4f}, MAE={r['mae']:.4f}")


if __name__ == "__main__":
    main()
