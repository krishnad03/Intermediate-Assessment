from pyspark.sql.functions import current_timestamp, input_file_name, col
from pyspark.sql.types import *
import os
import logging


def run_bronze(spark):

    BASE_DIR = os.getcwd()
    RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
    BRONZE_DIR = os.path.join(BASE_DIR, "data", "bronze")
    REJECTED_DIR = os.path.join(BRONZE_DIR, "quarantine")
    LOG_DIR = os.path.join(BASE_DIR, "data", "logs")

    os.makedirs(LOG_DIR, exist_ok=True)
    os.makedirs(BRONZE_DIR, exist_ok=True)
    os.makedirs(REJECTED_DIR, exist_ok=True)

    logging.basicConfig(
        filename=os.path.join(LOG_DIR, "bronze_ingestion.log"),
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

    customer_schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("region", StringType(), True),
        StructField("signup_date", StringType(), True),
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

    def ingest(file_name, folder, schema):

        df = (
            spark.read
            .option("header", True)
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            .schema(schema)
            .csv(os.path.join(RAW_DIR, file_name))
        )

        df = df.withColumn("ingestion_timestamp", current_timestamp()) \
               .withColumn("source_file", input_file_name())

        valid_df = df.filter(col("_corrupt_record").isNull()).drop("_corrupt_record")
        corrupt_df = df.filter(col("_corrupt_record").isNotNull())

        valid_df.write.mode("append").parquet(os.path.join(BRONZE_DIR, folder))

        if corrupt_df.limit(1).count() > 0:
            corrupt_df.write.mode("append").parquet(os.path.join(REJECTED_DIR, folder))
            logging.warning(f"Corrupt records in {file_name}")

        logging.info(f"{file_name} ingested")

    ingest("customers.csv", "customers", customer_schema)
    ingest("products.csv", "products", product_schema)
    ingest("transactions.csv", "transactions", transaction_schema)

    print("Bronze layer completed")
