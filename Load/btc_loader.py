from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("BTC Price ZScore Loader") \
    .config("spark.mongodb.connection.uri", "mongodb://mongodb:27017") \
    .config("spark.mongodb.database", "btc_analysis") \
    .getOrCreate()

# Define schema for the incoming Kafka messages
schema = StructType([
    StructField("timestamp", TimestampType(), True),
    StructField("price", DoubleType(), True),
    StructField("zscore", DoubleType(), True),
    StructField("window", StringType(), True)
])

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "btc-price-zscore") \
    .option("startingOffsets", "latest") \
    .load()

# Parse the JSON value and add processing timestamp
parsed_df = df.select(
    from_json(col("value").cast("string"), schema).alias("data"),
    current_timestamp().alias("processing_timestamp")
).select("data.*", "processing_timestamp")

# Define window sizes
window_sizes = ["30s", "1m", "5m", "15m", "30m", "1h"]

# Create a stream for each window size
for window_size in window_sizes:
    windowed_df = parsed_df \
        .withWatermark("timestamp", "10 seconds") \
        .groupBy(window("timestamp", window_size)) \
        .agg(
            {"price": "avg", "zscore": "avg"}
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("avg(price)").alias("avg_price"),
            col("avg(zscore)").alias("avg_zscore")
        )

    # Write to MongoDB
    query = windowed_df.writeStream \
        .format("mongodb") \
        .option("spark.mongodb.connection.uri", "mongodb://mongodb:27017") \
        .option("spark.mongodb.database", "btc_analysis") \
        .option("spark.mongodb.collection", f"btc-price-zscore-{window_size}") \
        .option("checkpointLocation", f"/tmp/checkpoints/btc-price-zscore-{window_size}") \
        .outputMode("append") \
        .start()

# Wait for the termination of all streams
spark.streams.awaitAnyTermination()
