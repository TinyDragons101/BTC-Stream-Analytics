from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType
import os

def main():    
    # Read Kafka connection and topic settings from environment variables
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
    checkpoint_loc = os.getenv("CHECKPOINT_LOADER", "/tmp/chk-loader")
    input_topic = os.getenv("ZSCORE_TOPIC", "btc-price-zscore")
    
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("BTC Price ZScore Loader") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.1.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.streaming.checkpointLocation", checkpoint_loc) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.mongodb.connection.uri", "mongodb://mongodb:27017") \
        .config("spark.mongodb.database", "btc_analysis") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Define the schema for the incoming Kafka messages
    schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("symbol", StringType(), True),
        StructField("zscores", ArrayType(
            StructType([
                StructField("window", StringType(), True),
                StructField("zscore_price", DoubleType(), True)
            ])
        ), True)
    ])

    # Read from Kafka
    read_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap) \
        .option("subscribe", input_topic) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .option("maxOffsetsPerTrigger", 1000) \
        .load()

    # Parse and transform the data
    transform_df = read_df \
        .select(from_json(col("value").cast("string"), schema).alias("data")) \
        .select("data.*")

    exploded_df = transform_df \
        .withColumn("zscore", explode(col("zscores"))) \
        .select(
            col("timestamp"),
            col("symbol"),
            col("zscore.window").alias("window"),
            col("zscore.zscore_price").alias("zscore_price")
        )

    # Write to MongoDB for each window size
    window_sizes = ['30s', '1m', '5m', '15m', '30m', '1h']

    # Start the streaming query
    query = exploded_df.writeStream \
        .foreachBatch(
            lambda df, epoch_id: [
                (
                    window_df.write
                        .format("mongo")
                        .mode("append")
                        .option("uri", f"mongodb://mongodb:27017/btc_analysis.btc-price-zscore-{window_size}")
                        .save(),
                )
                for window_size in window_sizes
                for window_df in [df.filter(col("window") == window_size).withColumn("_id", col("timestamp"))]
                if window_df.count() > 0
            ]
        ) \
        .option("checkpointLocation", checkpoint_loc) \
        .outputMode("append") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
