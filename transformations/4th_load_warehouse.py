from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os

# =========================================================
# PATH CONFIG
# =========================================================

BASE_DIR = os.getcwd()
SILVER_DIR = os.path.join(BASE_DIR, "data", "silver")

# =========================================================
# SPARK SESSION
# =========================================================

spark = (
    SparkSession.builder
    .appName("WarehouseLoadFinal")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# =========================================================
# POSTGRES CONFIG
# =========================================================

jdbc_url = "jdbc:postgresql://host.docker.internal:5433/de_warehouse"

connection_properties = {
    "user": "postgres",
    "password": "Kri12345@",
    "driver": "org.postgresql.Driver"
}

# =========================================================
# LOAD SILVER DATA (PARQUET)
# =========================================================

customers_df = spark.read.parquet(os.path.join(SILVER_DIR, "customers", "parquet"))
products_df = spark.read.parquet(os.path.join(SILVER_DIR, "products", "parquet"))
transactions_df = spark.read.parquet(os.path.join(SILVER_DIR, "transactions", "parquet"))

# =========================================================
# 1️⃣ LOAD DIM PRODUCT (APPEND)
# =========================================================

dim_product = (
    products_df
    .select("product_id", "product_name", "category", "price")
    .dropDuplicates(["product_id"])
)

dim_product.write.jdbc(
    url=jdbc_url,
    table="dim_product",
    mode="append",     # IMPORTANT
    properties=connection_properties
)

print("✅ dim_product loaded")

# =========================================================
# 2️⃣ LOAD DIM CUSTOMER (APPEND SCD FROM SILVER)
# =========================================================

dim_customer = (
    customers_df
    .select(
        "customer_id",
        "name",
        "region",
        "signup_date",
        "effective_from",
        "effective_to",
        "is_current"
    )
)

dim_customer.write.jdbc(
    url=jdbc_url,
    table="dim_customer",
    mode="append",    # IMPORTANT
    properties=connection_properties
)

print("✅ dim_customer loaded")

# =========================================================
# 3️⃣ LOAD FACT TABLE
# =========================================================

# Reload dimension tables with surrogate keys
dim_customer_db = spark.read.jdbc(
    url=jdbc_url,
    table="dim_customer",
    properties=connection_properties
)

dim_product_db = spark.read.jdbc(
    url=jdbc_url,
    table="dim_product",
    properties=connection_properties
)

# Join ONLY current customers (important for SCD Type 2)
current_customers = dim_customer_db.filter(col("is_current") == True)

fact_df = (
    transactions_df
    .join(
        current_customers.select("customer_id", "customer_sk"),
        on="customer_id",
        how="inner"        # ensure surrogate exists
    )
    .join(
        dim_product_db.select("product_id", "product_sk"),
        on="product_id",
        how="inner"
    )
    .select(
        "transaction_id",
        "customer_sk",
        "product_sk",
        "transaction_date",
        "amount",
        "status",
        "channel"
    )
)

# OPTIONAL: Truncate fact before reload (dev safe)
spark._jvm.java.sql.DriverManager.getConnection(
    jdbc_url,
    connection_properties["user"],
    connection_properties["password"]
).createStatement().execute("TRUNCATE TABLE fact_transactions")

fact_df.write.jdbc(
    url=jdbc_url,
    table="fact_transactions",
    mode="append",
    properties=connection_properties
)

print("✅ fact_transactions loaded")

spark.stop()
print("🎉 Warehouse Load Completed Successfully")
