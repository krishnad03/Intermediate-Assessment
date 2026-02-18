from pyspark.sql.functions import col, lower, to_date, row_number, year, month, current_date, lit
from pyspark.sql.window import Window
import os


def write_output(df, base_path, partition_cols=None):

    parquet_path = os.path.join(base_path, "parquet")
    csv_path = os.path.join(base_path, "csv")

    writer = df.write.mode("overwrite")

    if partition_cols:
        writer = writer.partitionBy(*partition_cols)

    writer.parquet(parquet_path)

    df.coalesce(1).write \
        .mode("overwrite") \
        .option("header", True) \
        .csv(csv_path)


def run_silver(spark):

    BASE_DIR = os.getcwd()
    BRONZE_DIR = os.path.join(BASE_DIR, "data", "bronze")
    SILVER_DIR = os.path.join(BASE_DIR, "data", "silver")

    os.makedirs(SILVER_DIR, exist_ok=True)

    # Customers SCD2
    bronze_customers = spark.read.parquet(os.path.join(BRONZE_DIR, "customers"))

    customers_silver = (
        bronze_customers
        .filter(col("customer_id").isNotNull())
        .withColumn("region", lower(col("region")))
        .withColumn("signup_date", to_date(col("signup_date")))
        .withColumn("effective_from", current_date())
        .withColumn("effective_to", lit(None).cast("date"))
        .withColumn("is_current", lit(True))
    )

    write_output(
        customers_silver,
        os.path.join(SILVER_DIR, "customers")
    )

    # Products
    bronze_products = spark.read.parquet(os.path.join(BRONZE_DIR, "products"))

    products_silver = (
        bronze_products
        .filter(col("product_id").isNotNull())
        .filter(col("price") > 0)
        .withColumn("category", lower(col("category")))
    )

    write_output(
        products_silver,
        os.path.join(SILVER_DIR, "products")
    )

    # Transactions
    bronze_transactions = spark.read.parquet(os.path.join(BRONZE_DIR, "transactions"))

    transactions_silver = (
        bronze_transactions
        .filter(col("transaction_id").isNotNull())
        .filter(col("amount") > 0)
        .withColumn("transaction_date", to_date(col("transaction_date")))
    )

    window_spec = Window.partitionBy("transaction_id") \
        .orderBy(col("ingestion_timestamp").desc())

    transactions_silver = transactions_silver \
        .withColumn("row_num", row_number().over(window_spec)) \
        .filter(col("row_num") == 1) \
        .drop("row_num")

    transactions_silver = transactions_silver \
        .withColumn("year", year("transaction_date")) \
        .withColumn("month", month("transaction_date"))

    write_output(
        transactions_silver,
        os.path.join(SILVER_DIR, "transactions"),
        partition_cols=["year", "month"]
    )

    print("Silver layer completed")
