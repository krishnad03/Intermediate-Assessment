from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import current_timestamp, input_file_name, col
import os
import logging


# Path Configuration
BASE_DIR = os.getcwd()

RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
BRONZE_DIR = os.path.join(BASE_DIR, "data", "bronze")
REJECTED_DIR = os.path.join(BRONZE_DIR, "rejected")
LOG_DIR = os.path.join(BASE_DIR, "data", "logs")

os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(BRONZE_DIR, exist_ok=True)
os.makedirs(REJECTED_DIR, exist_ok=True)

LOG_FILE_PATH = os.path.join(LOG_DIR, "bronze_ingestion.log")

logging.basicConfig(
    filename=LOG_FILE_PATH,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


# Spark Session
spark = (
    SparkSession.builder
    .appName("BronzeLayerIngestion")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


# Schemas (Match Raw Exactly)

customer_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("region", StringType(), True),
    StructField("signup_date", StringType(), True),
    StructField("is_current", StringType(), True),
    StructField("effective_from", StringType(), True),
    StructField("effective_to", StringType(), True),
    StructField("_corrupt_record", StringType(), True)
])

product_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("_corrupt_record", StringType(), True)
])

transaction_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("transaction_date", StringType(), True),
    StructField("status", StringType(), True),
    StructField("channel", StringType(), True),
    StructField("_corrupt_record", StringType(), True)
])


# Bronze Ingestion (Optimized)

def ingest_to_bronze(input_filename, output_folder, schema, partition_column=None):

    input_path = os.path.join(RAW_DIR, input_filename)
    output_path = os.path.join(BRONZE_DIR, output_folder)
    rejected_path = os.path.join(REJECTED_DIR, output_folder)

    df = (
        spark.read
        .option("header", True)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .schema(schema)
        .csv(input_path)
    )

    df = (
        df
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file", input_file_name())
    )

    valid_df = df.filter(col("_corrupt_record").isNull())
    corrupt_df = df.filter(col("_corrupt_record").isNotNull())

    # Write valid records
    if partition_column:
        valid_df.write.mode("overwrite").partitionBy(partition_column).parquet(output_path)
    else:
        valid_df.write.mode("overwrite").parquet(output_path)

    # Write corrupt if exists
    if corrupt_df.limit(1).count() > 0:
        corrupt_df.write.mode("overwrite").parquet(rejected_path)

    logging.info(f"Ingested {input_filename} successfully.")

    print(f" !! Bronze load completed for {input_filename} !!")


# Execute
if __name__ == "__main__":

    ingest_to_bronze("customers.csv", "customers", customer_schema)
    ingest_to_bronze("products.csv", "products", product_schema)
    ingest_to_bronze("transactions.csv", "transactions", transaction_schema, "transaction_date")

    spark.stop()
