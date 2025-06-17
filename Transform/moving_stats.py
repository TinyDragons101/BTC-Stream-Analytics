# File: lab4/Transform/moving_stats.py
# Spark Structured Streaming job to calculate moving average and standard deviation
# per sliding window for BTC price data.

import os
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.utils import StreamingQueryException
import time

def main():

    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("MovingStats") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/chk-moving") \
        .config("spark.sql.adaptive.enabled", "false") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("INFO")

    # Read Kafka connection and topic settings from environment variables
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
    input_topic = os.getenv("PRICE_TOPIC", "btc-price")
    output_topic = os.getenv("MOVING_TOPIC", "btc-price-moving")
    checkpoint_loc = os.getenv("CHECKPOINT_MOVING", "/tmp/chk-moving")

    # Read from Kafka topic
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap) \
        .option("subscribe", input_topic) \
        .option("startingOffsets", "latest") \
        .load() \
        
    # Parse JSON value and extract fields: symbol, price, event_time
    json_df = df.selectExpr("CAST(value AS STRING) AS json_str") \
        .select(
            F.from_json("json_str", 
                      "symbol STRING, price DOUBLE, event_time TIMESTAMP").alias("data")
        ) \
        .select("data.*") \
        .withColumnRenamed("event_time", "timestamp") \
        .filter(F.col("symbol").isNotNull() & F.col("price").isNotNull()) \
        .withWatermark("timestamp", "10 seconds")  # Handle late data up to 10 seconds
        
    # Define window sizes
    windows_config = [
        ("30 seconds", "30s"),
        ("1 minute", "1m"),
        ("5 minutes", "5m"),
        ("15 minutes", "15m"),
        ("30 minutes", "30m"),
        ("1 hour", "1h")
    ]

    # Create a list to hold DataFrames for each window size
    windowed_df = []

    # Create a stream for each window size
    for w in windows_config:
        windowed_df.append(
            json_df \
                .groupBy(
                    F.col("symbol"),
                    F.window("timestamp", w[0], "10 seconds").alias("window")
                ) \
                .agg(
                    F.avg("price").alias("avg_price"),
                    F.stddev("price").alias("std_price")
                ) \
                .select(
                    F.col("symbol"),
                    F.col("window.start").alias("window_start"),
                    F.col("window.end").alias("window_end"),
                    F.lit(w[1]).alias("window"),
                    F.col("avg_price"),
                    F.col("std_price")
                )
        )

    # Union all windowed DataFrames
    final_df = windowed_df[0]
    for other in windowed_df[1:]:
        final_df = final_df.union(other)

    # Process and write to Kafka
    final_df.writeStream \
        .foreachBatch(lambda batch_df, batch_id: (
            batch_df \
                .groupBy("symbol", "window_start") \
                .agg(
                    F.collect_list(
                        F.struct(
                            F.col("window"),
                            F.col("avg_price"),
                            F.col("std_price"),
                            # F.date_format(F.col("window_end"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("end_time")
                        )
                    ).alias("windows")
                ) \
                .select(
                    F.col("symbol"),
                    F.to_json(
                        F.struct(
                            F.date_format(F.col("window_start"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("timestamp"),
                            F.col("symbol"),
                            F.col("windows")
                        )
                    ).alias("value")
                ) \
                .write \
                .format("kafka") \
                .option("kafka.bootstrap.servers", kafka_bootstrap) \
                .option("topic", output_topic) \
                .save()
        ) if not batch_df.rdd.isEmpty() else None) \
        .outputMode("update") \
        .trigger(processingTime="10 seconds") \
        .start() \
        .awaitTermination()
        
if __name__ == "__main__":
    main()
