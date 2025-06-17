# File: lab4/Transform/moving_stats.py
# Spark Structured Streaming job to calculate moving average and standard deviation
# per sliding window for BTC price data.

import os
from pyspark.sql import SparkSession, functions as F
import logging

def main():
    # Logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger("MovingStatsLogger")
    logger.info("Starting Moving Stats job...")

    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("MovingStats") \
        .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("INFO")

    # Read Kafka connection and topic settings from environment variables
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
    input_topic = os.getenv("PRICE_TOPIC", "btc-price")
    output_topic = os.getenv("MOVING_TOPIC", "btc-price-moving")
    checkpoint_loc = os.getenv("CHECKPOINT_MOVING", "/tmp/chk-moving")

    # Read streaming data from Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap) \
        .option("subscribe", input_topic) \
        .option("startingOffsets", "latest") \
        .load()
    
    # Parse JSON value and extract fields: symbol, price, event_time
    json_df = df.selectExpr("CAST(value AS STRING) AS json_str") \
        .select(
            F.from_json("json_str", 
                      "symbol STRING, price DOUBLE, event_time TIMESTAMP").alias("data")
        ) \
        .select("data.*") \
        .withWatermark("event_time", "20 seconds")

    # Log the schema of the parsed DataFrame
    logger.info("Parsed DataFrame schema: %s", json_df.schema)

    # Define sliding windows for aggregation
    window_config = [
        ("30 seconds", "30s"),
        ("1 minute", "1m"),
        ("5 minutes", "5m"),
        ("15 minutes", "15m"),
        ("30 minutes", "30m"),
        ("1 hour", "1h")
    ]

    # Create a list to store window expressions
    window_exprs = []
    for w in window_config:
        window_exprs.append(
            F.struct(
                F.lit(w[1]).alias("window"),
                F.avg("price").over(
                    F.window("event_time", w[0], w[0])
                ).alias("avg_price"),
                F.stddev_samp("price").over(
                    F.window("event_time", w[0], w[0])
                ).alias("std_price")
            )
        )

    # Calculate all window statistics in one pass
    result_df = json_df.select(
        "symbol",
        F.col("event_time").alias("timestamp"),
        F.array(*window_exprs).alias("windows")
    )

    # Convert to JSON string for Kafka output
    out_df = result_df.selectExpr("to_json(struct(timestamp, symbol, windows)) AS value")

    # Reduce partitions to avoid creating too many Kafka tasks
    out_df = out_df.coalesce(2)

    kafka_query = out_df.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap) \
        .option("topic", output_topic) \
        .option("checkpointLocation", checkpoint_loc) \
        .outputMode("complete") \
        .trigger(processingTime="5 seconds") \
        .start()

    # Wait for all queries to finish
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
