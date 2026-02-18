from pyspark.sql.functions import col, sum as _sum, count, desc, avg, when
from pyspark.sql.functions import broadcast
import os


def write_output(df, base_path):

    parquet_path = os.path.join(base_path, "parquet")
    csv_path = os.path.join(base_path, "csv")

    df.write.mode("overwrite").parquet(parquet_path)

    df.coalesce(1).write \
        .mode("overwrite") \
        .option("header", True) \
        .csv(csv_path)


def run_gold(spark):

    BASE_DIR = os.getcwd()
    SILVER_DIR = os.path.join(BASE_DIR, "data", "silver")
    GOLD_DIR = os.path.join(BASE_DIR, "data", "gold")

    os.makedirs(GOLD_DIR, exist_ok=True)

    customers = spark.read.parquet(os.path.join(SILVER_DIR, "customers", "parquet"))
    products = spark.read.parquet(os.path.join(SILVER_DIR, "products", "parquet"))
    transactions = spark.read.parquet(os.path.join(SILVER_DIR, "transactions", "parquet"))

    # Monthly revenue
    monthly_revenue = (
        transactions
        .groupBy("year", "month")
        .agg(_sum("amount").alias("total_revenue"))
        .orderBy("year", "month")
    )

    write_output(
        monthly_revenue,
        os.path.join(GOLD_DIR, "monthly_revenue")
    )

    # Regional performance
    current_customers = customers.filter(col("is_current") == True)

    regional_perf = (
        transactions
        .join(
            broadcast(current_customers.select("customer_id", "region")),
            "customer_id",
            "inner"
        )
        .groupBy("region")
        .agg(
            _sum("amount").alias("regional_revenue"),
            count("transaction_id").alias("total_transactions")
        )
        .orderBy(desc("regional_revenue"))
    )

    write_output(
        regional_perf,
        os.path.join(GOLD_DIR, "regional_performance")
    )

    print("Gold layer completed")
