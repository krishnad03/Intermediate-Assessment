# config.py

# ===========================
# JDBC CONFIG
# ===========================
JDBC_URL = "jdbc:postgresql://host.docker.internal:5433/de_warehouse"

CONNECTION_PROPERTIES = {
    "user": "postgres",
    "password": "Kri12345@",
    "driver": "org.postgresql.Driver"
}

# ===========================
# DIMENSION COLUMNS
# ===========================
DIM_PRODUCT_COLUMNS = [
    "product_id",
    "product_name",
    "category",
    "price"
]

DIM_CUSTOMER_COLUMNS = [
    "customer_id",
    "name",
    "region",
    "signup_date",
    "effective_from",
    "effective_to",
    "is_current"
]

FACT_COLUMNS = [
    "transaction_id",
    "customer_sk",
    "product_sk",
    "transaction_date",
    "amount",
    "status",
    "channel"
]
