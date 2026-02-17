from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lower,
    to_date,
    row_number,
    when,
    lead,
    year,
    month
)
from pyspark.sql.window import Window
import os

# Path Config
BASE_DIR = os.getcwd()

BRONZE_DIR = os.path.join(BASE_DIR, "data", "bronze")
SILVER_DIR = os.path.join(BASE_DIR, "data", "silver")

os.makedirs(SILVER_DIR, exist_ok=True)

# Spark Session
spark = (SparkSession.builder.appName("SilverLayerWithSCD").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")


# Helper Function: Write Parquet + CSV
def write_output(df, folder_name, partition_cols=None):

    parquet_path = os.path.join(SILVER_DIR, folder_name, "parquet")
    csv_path = os.path.join(SILVER_DIR, folder_name, "csv")

    # Write parquet
    writer = df.write.mode("overwrite")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)

    writer.parquet(parquet_path)

    # Write CSV (single file for readability)
    df.coalesce(1).write \
        .mode("overwrite") \
        .option("header", True) \
        .csv(csv_path)


# Customers - SCD Type 2 Enforcement
def transform_customers():

    df = spark.read.parquet(os.path.join(BRONZE_DIR, "customers"))
    df = (
        df
        .filter(col("customer_id").isNotNull())
        .withColumn("region", lower(col("region")))
        .withColumn("signup_date", to_date(col("signup_date")))
        .withColumn("effective_from", to_date(col("effective_from")))
        .withColumn("effective_to", to_date(col("effective_to")))
        .withColumn("is_current", col("is_current").cast("boolean"))
    )

    # SCD ordering
    window_spec = Window.partitionBy("customer_id").orderBy("effective_from")
    df = df.withColumn("next_effective_from", lead("effective_from").over(window_spec))

    # Auto-close previous record
    df = df.withColumn(
        "effective_to",
        when(
            col("effective_to").isNull() & col("next_effective_from").isNotNull(),
            col("next_effective_from")
        ).otherwise(col("effective_to"))
    )

    # Recalculate is_current
    df = df.withColumn(
        "is_current",
        when(col("next_effective_from").isNull(), True).otherwise(False)
    )

    df = df.drop("next_effective_from")

    write_output(df, "customers")
    print("!! Silver customers with SCD Type 2 ready")

# Products
def transform_products():

    df = spark.read.parquet(os.path.join(BRONZE_DIR, "products"))
    df = (
        df
        .filter(col("product_id").isNotNull())
        .filter(col("price") > 0)
        .withColumn("category", lower(col("category")))
    )

    write_output(df, "products")

    print("!! **Silver products ready** !!")

# Transactions (Dedup + Cleaning)
def transform_transactions():

    df = spark.read.parquet(os.path.join(BRONZE_DIR, "transactions"))
    df = (
        df
        .filter(col("transaction_id").isNotNull())
        .filter(col("amount") > 0)
        .withColumn("transaction_date", to_date(col("transaction_date")))
        .withColumn("status", lower(col("status")))
    )

    window_spec = Window.partitionBy("transaction_id") \
        .orderBy(col("ingestion_timestamp").desc())

    df = (
        df
        .withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )

    # Add partition columns
    df = df.withColumn("year", year("transaction_date")) \
           .withColumn("month", month("transaction_date"))

    write_output(df, "transactions", partition_cols=["year", "month"])

    print("!! ***Silver transactions ready*** !!")

# Execute
if __name__ == "__main__":

    transform_customers()
    transform_products()
    transform_transactions()

    spark.stop()
