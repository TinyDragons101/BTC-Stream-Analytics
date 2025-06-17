# File: lab4/Transform/zscore.py
# Spark Structured Streaming job to calculate Z-scores for BTC price data
# using moving statistics from btc-price-moving topic

import os
from pyspark.sql import SparkSession, functions as F
import logging

def main():
    # Logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger("ZScoreLogger")
    logger.info("Starting Z-Score job...")

    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("ZScore") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("INFO")

    # Read Kafka connection and topic settings from environment variables
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
    price_topic = os.getenv("PRICE_TOPIC", "btc-price")
    moving_topic = os.getenv("MOVING_TOPIC", "btc-price-moving")
    output_topic = os.getenv("ZSCORE_TOPIC", "btc-price-zscore")
    checkpoint_loc = os.getenv("CHECKPOINT_ZSCORE", "/tmp/chk-zscore")

    # Read price data from Kafka
    price_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap) \
        .option("subscribe", price_topic) \
        .option("startingOffsets", "latest") \
        .load()

    # Read moving statistics from Kafka
    moving_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap) \
        .option("subscribe", moving_topic) \
        .option("startingOffsets", "latest") \
        .load()

    # Parse price data
    price_parsed = price_df.selectExpr("CAST(value AS STRING) AS json_str") \
        .select(
            F.from_json("json_str", 
                      "symbol STRING, price DOUBLE, event_time TIMESTAMP").alias("data")
        ) \
        .select("data.*") \
        .withWatermark("event_time", "10 seconds")

    # Parse moving statistics data
    moving_parsed = moving_df.selectExpr("CAST(value AS STRING) AS json_str") \
        .select(
            F.from_json("json_str", 
                      "timestamp TIMESTAMP, symbol STRING, windows ARRAY<STRUCT<window: STRING, avg_price: DOUBLE, std_price: DOUBLE>>").alias("data")
        ) \
        .select("data.*") \
        .withWatermark("timestamp", "10 seconds")

    # Explode windows array to get individual window statistics
    moving_exploded = moving_parsed.select(
        "timestamp",
        "symbol",
        F.explode("windows").alias("window_stats")
    ).select(
        "timestamp",
        "symbol",
        "window_stats.window",
        "window_stats.avg_price",
        "window_stats.std_price"
    )

    # Join price data with moving statistics on timestamp and symbol
    joined_df = price_parsed.join(
        moving_exploded,
        (F.to_timestamp(price_parsed.event_time) == F.to_timestamp(moving_exploded.timestamp)) &
        (price_parsed.symbol == moving_exploded.symbol),
        "inner"
    )

    # Calculate Z-scores
    zscore_df = joined_df.select(
        "timestamp",
        "symbol",
        "window",
        F.when(F.col("std_price") == 0, 0)  # Handle case where std_price is 0
        .otherwise((F.col("price") - F.col("avg_price")) / F.col("std_price"))
        .alias("zscore_price")
    )

    # Group by timestamp and symbol to create zscores array
    final_df = zscore_df.groupBy("timestamp", "symbol") \
        .agg(
            F.collect_list(
                F.struct(
                    "window",
                    "zscore_price"
                )
            ).alias("zscores")
        )

    # Convert to JSON string for Kafka output
    out_df = final_df.selectExpr("to_json(struct(timestamp, symbol, zscores)) AS value")

    # Reduce partitions to avoid creating too many Kafka tasks
    out_df = out_df.coalesce(2)

    kafka_query = out_df.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap) \
        .option("topic", output_topic) \
        .option("checkpointLocation", checkpoint_loc) \
        .outputMode("append") \
        .trigger(processingTime="5 seconds") \
        .start()

    # Wait for all queries to finish
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()