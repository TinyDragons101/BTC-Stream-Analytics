# File: lab4/Transform/zscore.py
# Spark Structured Streaming job to compute z-score for BTC price data
# by joining raw price stream with moving statistics.

import os
from pyspark.sql import SparkSession, functions as F
import logging

def main():
    # Logging setup
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s- %(message)s')
    logger = logging.getLogger("ZScoreLogger")
    logger.info("Starting Z-Score computation job...")

    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("ZScore") \
        .getOrCreate()

    # Environment variable settings
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
    price_topic     = os.getenv("PRICE_TOPIC", "btc-price")
    moving_topic    = os.getenv("MOVING_TOPIC", "btc-price-moving")
    output_topic    = os.getenv("ZSCORE_TOPIC", "btc-price-zscore")
    checkpoint_loc  = os.getenv("CHECKPOINT_ZSCORE", "/tmp/chk-zscore")

    # Read BTC price stream from Kafka and parse JSON
    price_df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_bootstrap) \
            .option("subscribe", price_topic) \
            .option("startingOffsets", "latest") \
            .load() \
            .selectExpr("CAST(value AS STRING) AS json_str") \
            .select(
                F.from_json(
                    "json_str",
                    "symbol STRING, price DOUBLE, event_time TIMESTAMP"
                ).alias("data")
            ) \
            .select("data.symbol", "data.price", "data.event_time") \
            .withColumnRenamed("event_time", "timestamp") \
            .withWatermark("timestamp", "10 seconds")

    # Read moving statistics stream from Kafka and parse JSON
    moving_df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_bootstrap) \
            .option("subscribe", moving_topic) \
            .option("startingOffsets", "latest") \
            .load() \
            .selectExpr("CAST(value AS STRING) AS json_str") \
            .select(
                F.from_json(
                    "json_str",
                    "timestamp TIMESTAMP, symbol STRING, windows ARRAY<STRUCT<window: STRING, avg_price: DOUBLE, std_price: DOUBLE>>"
                ).alias("data")
            ) \
            .select("data.timestamp", "data.symbol", "data.windows") \
            .withWatermark("timestamp", "10 seconds")

    # Explode the windows array to get individual window records
    moving_df = moving_df.select(
        "timestamp",
        "symbol",
        F.explode("windows").alias("window_data")
    ).select(
        "timestamp",
        "symbol",
        "window_data.window",
        "window_data.avg_price",
        "window_data.std_price"
    )

    # Perform stream-stream join on the timestamp field
    joined_df = price_df.join(
        moving_df,
        (price_df.symbol == moving_df.symbol) & 
        (price_df.timestamp == moving_df.timestamp),
        how="inner"
    )

    # Calculate z-score for each window
    zscore_df = joined_df \
        .withColumn("zscore_price",
            F.when(
                (F.col("std_price").isNotNull()) & (F.col("std_price") > 0),
                (F.col("price") - F.col("avg_price")) / F.col("std_price")
            ).otherwise(0.0)
        )

    # Group by timestamp and symbol to create the nested array structure
    final_df = zscore_df \
        .groupBy("timestamp", "symbol") \
        .agg(
            F.collect_list(
                F.struct(
                    F.col("window"),
                    F.col("zscore_price")
                )
            ).alias("zscores")
        ) \
        .select(
            F.to_json(
                F.struct(
                    F.date_format("timestamp", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("timestamp"),
                    "symbol",
                    "zscores"
                )
            ).alias("value")
        )

    # Write z-score records to Kafka
    final_df.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap) \
        .option("topic", output_topic) \
        .outputMode("append") \
        .trigger(processingTime="10 seconds") \
        .start() \
        .awaitTermination()

if __name__ == "__main__":
    main()