#!/usr/bin/env python3
"""
Spark Structured Streaming job.
Reads from Kafka topic 'station_status', enriches with computed fields,
and writes to TimescaleDB in micro-batches every 10 seconds.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, when, lit, current_timestamp, to_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, BooleanType, TimestampType
)

# --- Config from env ---
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
PG_HOST      = os.getenv("POSTGRES_HOST", "timescaledb")
PG_PORT      = os.getenv("POSTGRES_PORT", "5432")
PG_USER      = os.getenv("POSTGRES_USER", "bikestream")
PG_PASSWORD  = os.getenv("POSTGRES_PASSWORD", "bikestream_2024")
PG_DB        = os.getenv("POSTGRES_DB", "bikestream")
PG_URL       = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"

# --- Kafka message schema ---
STATION_SCHEMA = StructType([
    StructField("city",                   StringType()),
    StructField("system",                 StringType()),
    StructField("station_id",             StringType()),
    StructField("timestamp",              StringType()),
    StructField("num_bikes_available",    IntegerType()),
    StructField("num_docks_available",    IntegerType()),
    StructField("num_ebikes_available",   IntegerType()),
    StructField("num_scooters_available", IntegerType()),
    StructField("is_renting",             BooleanType()),
    StructField("is_returning",           BooleanType()),
    StructField("station_name",           StringType()),
    StructField("lat",                    DoubleType()),
    StructField("lon",                    DoubleType()),
    StructField("capacity",              IntegerType()),
])


def write_to_timescaledb(batch_df, batch_id):
    """Write each micro-batch to TimescaleDB via JDBC."""
    if batch_df.count() == 0:
        return

    # Compute fill_ratio and status
    enriched = batch_df \
        .withColumn("time", to_timestamp(col("timestamp"))) \
        .withColumn("total_slots",
            col("num_bikes_available") + col("num_docks_available")) \
        .withColumn("fill_ratio",
            when(col("total_slots") > 0,
                 col("num_bikes_available") / col("total_slots"))
            .otherwise(lit(0.0))) \
        .withColumn("status",
            when(col("num_bikes_available") == 0, lit("EMPTY"))
            .when(col("fill_ratio") < 0.2,        lit("LOW"))
            .when(col("fill_ratio") > 0.9,         lit("HIGH"))
            .when(col("num_docks_available") == 0, lit("FULL"))
            .otherwise(lit("HEALTHY"))) \
        .withColumn("needs_rebalancing",
            (col("status") == "EMPTY") | (col("status") == "FULL")) \
        .select(
            "time", "city", "station_id", "station_name",
            "lat", "lon", "capacity",
            "num_bikes_available", "num_docks_available",
            "num_ebikes_available", "num_scooters_available",
            "fill_ratio", "status", "needs_rebalancing"
        )

    # Write to station_snapshots table
    enriched.write \
        .format("jdbc") \
        .option("url", PG_URL) \
        .option("dbtable", "station_snapshots") \
        .option("user", PG_USER) \
        .option("password", PG_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

    print(f"[Batch {batch_id}] Wrote {enriched.count()} rows to TimescaleDB")


def main():
    spark = SparkSession.builder \
        .appName("BikeStream-Processor") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,"
                "org.postgresql:postgresql:42.7.3") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Read from Kafka
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", "station_status") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    # Parse JSON
    parsed = raw_stream \
        .select(from_json(
            col("value").cast("string"),
            STATION_SCHEMA
        ).alias("data")) \
        .select("data.*")

    # Write in micro-batches
    query = parsed.writeStream \
        .foreachBatch(write_to_timescaledb) \
        .trigger(processingTime="10 seconds") \
        .option("checkpointLocation", "/tmp/bikestream_checkpoint") \
        .start()

    query.awaitTermination()


if __name__ == "__main__":
    main()
