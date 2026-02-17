from pyspark.sql import SparkSession
from pyspark.sql.functions import (col,to_timestamp,window,sum as _sum,when)
import os

# PATH CONFIG
BASE_DIR = os.getcwd()
STREAM_DIR = os.path.join(BASE_DIR, "data", "streaming")
OUTPUT_DIR = os.path.join(BASE_DIR, "data", "gold", "streaming_revenue")
CHECKPOINT_DIR = os.path.join(BASE_DIR, "data", "checkpoints")

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(CHECKPOINT_DIR, exist_ok=True)

# SPARK SESSION
spark = (SparkSession.builder.appName("StructuredStreamingRevenue").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

# Define Schema (Important for Streaming)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("transaction_date", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("status", StringType(), True),
    StructField("channel", StringType(), True)
])

# Read Streaming Data
stream_df = (spark.readStream.schema(schema).json(STREAM_DIR))

# Convert to timestamp (event time)
stream_df = stream_df.withColumn("event_time",to_timestamp(col("transaction_date")))


# Apply Watermark + Window Aggregation
windowed_revenue = (stream_df.withWatermark("event_time", "10 minutes").groupBy(window(col("event_time"), "5 minutes")).agg(_sum("amount").alias("total_revenue")))

# Real-time Fraud Flag (High Value)
fraud_stream = (stream_df.withColumn("fraud_flag",when(col("amount") > 10000, "HIGH_VALUE").otherwise("NORMAL")).filter(col("fraud_flag") == "HIGH_VALUE"))


# Write Streaming Output
revenue_query = (
    windowed_revenue
    .writeStream
    .outputMode("append")
    .format("parquet")
    .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "revenue"))
    .option("path", OUTPUT_DIR)
    .start()
)

fraud_query = (
    fraud_stream
    .writeStream
    .outputMode("append")
    .format("parquet")
    .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "fraud"))
    .option("path", os.path.join(BASE_DIR, "data", "gold", "streaming_fraud"))
    .start()
)

print(" Streaming started !!")

revenue_query.awaitTermination()
fraud_query.awaitTermination()
