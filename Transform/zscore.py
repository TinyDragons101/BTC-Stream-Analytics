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
    price_df = (
        spark.readStream
             .format("kafka")
             .option("kafka.bootstrap.servers", kafka_bootstrap)
             .option("subscribe", price_topic)
             .option("startingOffsets", "latest")
             .load()
             .selectExpr("CAST(value AS STRING) AS json_str")
             .select(
                 F.from_json(
                     "json_str",
                     "symbol STRING, price DOUBLE, event_time TIMESTAMP"
                 ).alias("data")
             )
             .select("data.symbol", "data.price", "data.event_time")
             # Rename for join consistency
             .withColumnRenamed("event_time", "timestamp")
             .withWatermark("timestamp", "10 seconds")
    )

    # Read moving statistics stream from Kafka and parse JSON
    moving_df = (
        spark.readStream
             .format("kafka")
             .option("kafka.bootstrap.servers", kafka_bootstrap)
             .option("subscribe", moving_topic)
             .option("startingOffsets", "latest")
             .load()
             .selectExpr("CAST(value AS STRING) AS json_str")
             .select(
                 F.from_json(
                     "json_str",
                     "timestamp TIMESTAMP, window STRING, avg_price DOUBLE, std_price DOUBLE"
                 ).alias("data")
             )
             .select("data.timestamp", "data.window", "data.avg_price", "data.std_price")
             .withWatermark("timestamp", "10 seconds")
    )

    # Perform stream-stream join on the timestamp field
    joined = price_df.join(
        moving_df,
        on=["timestamp"],
        how="inner"  # Inner join ensures we only get aligned records
    )

    # Log the schema of the joined DataFrame
    logger.info("Joined DataFrame schema: %s", joined.schema)

    # Compute z-score and handle division by zero
    zscore_df = (
        joined
          .withColumn(
              "z_score",
              F.when(
                  F.col("std_price") > 0,
                  (F.col("price") - F.col("avg_price")) / F.col("std_price")
              ).otherwise(0.0)
          )
          # Prepare JSON output for Kafka
          .selectExpr(
              "to_json(struct(symbol, timestamp, window, price, z_score)) AS value"
          )
    )

    # Write z-score records to Kafka
    (zscore_df.writeStream
             .format("kafka")
             .option("kafka.bootstrap.servers", kafka_bootstrap)
             .option("topic", output_topic)
             .option("checkpointLocation", checkpoint_loc)
             .outputMode("append")
             .start()
             .awaitTermination())


if __name__ == "__main__":
    main()