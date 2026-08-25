#!/usr/bin/env python3
"""
Second Spark job: reads from Kafka, computes 5-minute windowed
system metrics per city, and writes to system_metrics table.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_timestamp, window,
    count, sum as spark_sum, avg, when
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, BooleanType
)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
PG_HOST      = os.getenv("POSTGRES_HOST", "timescaledb")
PG_PORT      = os.getenv("POSTGRES_PORT", "5432")
PG_USER      = os.getenv("POSTGRES_USER", "bikestream")
PG_PASSWORD  = os.getenv("POSTGRES_PASSWORD", "bikestream_2024")
PG_DB        = os.getenv("POSTGRES_DB", "bikestream")
PG_URL       = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"

SCHEMA = StructType([
    StructField("city",                   StringType()),
    StructField("station_id",             StringType()),
    StructField("timestamp",              StringType()),
    StructField("num_bikes_available",    IntegerType()),
    StructField("num_docks_available",    IntegerType()),
    StructField("num_ebikes_available",   IntegerType()),
    StructField("num_scooters_available", IntegerType()),
    StructField("capacity",              IntegerType()),
])


def write_metrics(batch_df, batch_id):
    if batch_df.count() == 0:
        return

    batch_df.write \
        .format("jdbc") \
        .option("url", PG_URL) \
        .option("dbtable", "system_metrics") \
        .option("user", PG_USER) \
        .option("password", PG_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

    print(f"[Metrics Batch {batch_id}] Wrote {batch_df.count()} aggregate rows")


def main():
    spark = SparkSession.builder \
        .appName("BikeStream-MetricsAggregator") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,"
                "org.postgresql:postgresql:42.7.3") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", "station_status") \
        .option("startingOffsets", "latest") \
        .load()

    parsed = raw \
        .select(from_json(col("value").cast("string"), SCHEMA).alias("d")) \
        .select("d.*") \
        .withColumn("event_time", to_timestamp(col("timestamp"))) \
        .withColumn("total_slots",
            col("num_bikes_available") + col("num_docks_available")) \
        .withColumn("fill_ratio",
            when(col("total_slots") > 0,
                 col("num_bikes_available") / col("total_slots"))
            .otherwise(0.0)) \
        .withColumn("is_empty", (col("num_bikes_available") == 0).cast("int")) \
        .withColumn("is_full",  (col("num_docks_available") == 0).cast("int"))

    # 5-minute tumbling window aggregation per city
    agg = parsed \
        .withWatermark("event_time", "1 minute") \
        .groupBy(
            window(col("event_time"), "5 minutes"),
            col("city")
        ) \
        .agg(
            spark_sum("num_bikes_available").alias("total_bikes"),
            spark_sum("num_docks_available").alias("total_docks"),
            spark_sum("num_ebikes_available").alias("total_ebikes"),
            avg("fill_ratio").alias("avg_fill_ratio"),
            spark_sum("is_empty").alias("num_empty_stations"),
            spark_sum("is_full").alias("num_full_stations"),
            count("*").alias("station_count"),
        ) \
        .withColumn("time", col("window.start")) \
        .withColumn("utilization_pct", col("avg_fill_ratio") * 100) \
        .select(
            "time", "city", "total_bikes", "total_docks", "total_ebikes",
            "avg_fill_ratio", "num_empty_stations", "num_full_stations",
            "utilization_pct"
        )

    query = agg.writeStream \
        .foreachBatch(write_metrics) \
        .trigger(processingTime="5 minutes") \
        .option("checkpointLocation", "/tmp/bikestream_metrics_checkpoint") \
        .outputMode("update") \
        .start()

    query.awaitTermination()


if __name__ == "__main__":
    main()
