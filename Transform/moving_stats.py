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
        .withWatermark("event_time", "10 seconds") \

    # Log the schema of the parsed DataFrame
    logger.info("Parsed DataFrame schema: %s", json_df.schema)

    # Define sliding windows for aggregation
    windows = ["30 seconds", "1 minute", "5 minutes", "15 minutes", "30 minutes", "1 hour"]
    agg_dfs = []

    # For each window size, compute average and standard deviation
    for w in windows:
        win_df = (
            json_df
              .groupBy(F.window("event_time", w, w).alias("window"))
              .agg(
                  F.avg("price").alias("avg_price"),
                  F.stddev_samp("price").alias("std_price")
              )
              # Select desired output schema: timestamp end, window label, metrics
              .selectExpr(
                  "window.end AS timestamp",
                  f"'{w}' AS window",
                  "avg_price",
                  "std_price"
              )
        )
        agg_dfs.append(win_df)

    # Union all windowed DataFrames into one
    result_df = agg_dfs[0]
    for other in agg_dfs[1:]:
        result_df = result_df.union(other)

    # Giảm số partition để tránh tạo hàng trăm Kafka tasks
    result_df = result_df.coalesce(2)

    # Convert struct to JSON string for Kafka output
    out_df = result_df.selectExpr("to_json(struct(timestamp, window, avg_price, std_price)) AS value")


    kafka_query = out_df.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap) \
        .option("topic", output_topic) \
        .option("checkpointLocation", checkpoint_loc) \
        .outputMode("append") \
        .trigger(processingTime="5 seconds") \
        .start()

    # Đợi tất cả query chạy xong
    spark.streams.awaitAnyTermination()
    

if __name__ == "__main__":
    main()
