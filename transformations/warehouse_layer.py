from pyspark.sql.functions import col
import os
from config import JDBC_URL, CONNECTION_PROPERTIES, DIM_PRODUCT_COLUMNS, DIM_CUSTOMER_COLUMNS


def run_warehouse(spark):

    BASE_DIR = os.getcwd()
    SILVER_DIR = os.path.join(BASE_DIR, "data", "silver")

    customers_df = spark.read.parquet(os.path.join(SILVER_DIR, "customers", "parquet"))
    products_df = spark.read.parquet(os.path.join(SILVER_DIR, "products", "parquet"))

    dim_product = products_df.select(*DIM_PRODUCT_COLUMNS).dropDuplicates(["product_id"])

    dim_product.write.jdbc(
        url=JDBC_URL,
        table="dim_product",
        mode="append",
        properties=CONNECTION_PROPERTIES
    )

    dim_customer = customers_df.select(*DIM_CUSTOMER_COLUMNS)

    dim_customer.write.jdbc(
        url=JDBC_URL,
        table="dim_customer",
        mode="append",
        properties=CONNECTION_PROPERTIES
    )

    print("Warehouse load completed")
