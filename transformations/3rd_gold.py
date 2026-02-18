from pyspark.sql import SparkSession
from pyspark.sql.functions import (col,sum as _sum,count,desc,percent_rank,when,lag,avg)
from pyspark.sql.window import Window
from pyspark.sql.functions import broadcast
import os


# PATH CONFIG
BASE_DIR = os.getcwd()
SILVER_DIR = os.path.join(BASE_DIR, "data", "silver")
GOLD_DIR = os.path.join(BASE_DIR, "data", "gold")

os.makedirs(GOLD_DIR, exist_ok=True)

# SPARK SESSION
spark = (SparkSession.builder.appName("GoldLayerAdvanced").getOrCreate())

spark.sparkContext.setLogLevel("ERROR")

# LOAD SILVER TABLES
customers = spark.read.parquet(os.path.join(SILVER_DIR, "customers", "parquet"))
products = spark.read.parquet(os.path.join(SILVER_DIR, "products", "parquet"))
transactions = spark.read.parquet(os.path.join(SILVER_DIR, "transactions", "parquet"))


# 1️ Monthly Revenue (Partition Aware)
monthly_revenue = (
    transactions
    .groupBy("year", "month")
    .agg(_sum("amount").alias("total_revenue"))
    .orderBy("year", "month")
)

monthly_revenue.write.mode("overwrite") \
    .parquet(os.path.join(GOLD_DIR, "monthly_revenue"))

print("!! Monthly revenue ready !!")


# 2️ Top Products (Broadcast Join)
top_products = (
    transactions
    .groupBy("product_id")
    .agg(_sum("amount").alias("total_sales"))
    .join(broadcast(products), "product_id", "left")
    .orderBy(desc("total_sales"))
)

top_products.write.mode("overwrite") \
    .parquet(os.path.join(GOLD_DIR, "top_products"))

print("!! Top products ready !!")


# 3️ Regional Performance (Only Current Customers)


current_customers = customers.filter(col("is_current") == True)

regional_perf = (
    transactions
    .join(broadcast(current_customers), "customer_id", "inner")
    .groupBy("region")
    .agg(
        _sum("amount").alias("regional_revenue"),
        count("transaction_id").alias("total_transactions")
    )
    .orderBy(desc("regional_revenue"))
)

regional_perf.write.mode("overwrite") \
    .parquet(os.path.join(GOLD_DIR, "regional_performance"))

print("!! Regional performance ready !!")

# 4️ Advanced Fraud Detection (Customer Spike Logic)
customer_window = Window.partitionBy("customer_id").orderBy("transaction_date")

fraud_df = (
    transactions
    .withColumn("previous_amount", lag("amount").over(customer_window))
    .withColumn("avg_customer_spend",
                avg("amount").over(Window.partitionBy("customer_id")))
    .withColumn(
        "fraud_flag",
        when(col("amount") > col("avg_customer_spend") * 3, "SPIKE")
        .otherwise("NORMAL")
    )
)

fraud_transactions = fraud_df.filter(col("fraud_flag") == "SPIKE")

fraud_transactions.write.mode("overwrite") \
    .parquet(os.path.join(GOLD_DIR, "fraud_transactions"))

print("!! Fraud spike dataset ready !!")

spark.stop()
print("**** Gold Layer Completed Successfully ****")
