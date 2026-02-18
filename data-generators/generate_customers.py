import pandas as pd
import random
from datetime import datetime, timedelta
import os



N = 50000
regions = ["North", "South", "East", "West", "Central"]
start_date = datetime(2024, 1, 1)

# Ensure output directory exists
# Get current script directory
base_dir = os.path.dirname(os.path.abspath(__file__))
# Go to project root (one folder up)
project_root = os.path.abspath(os.path.join(base_dir, ".."))
# Create data/raw folder
output_path = os.path.join(project_root, "data", "raw")
os.makedirs(output_path, exist_ok=True)


# Base Customer Records
data = []

for i in range(1, N + 1):
    signup = start_date + timedelta(days=random.randint(0, 700))

    data.append([
        i,
        f"Customer{i}",
        random.choice(regions),
        signup.strftime("%Y-%m-%d"),
        True,
        signup.strftime("%Y-%m-%d"),
        None
    ])

columns = [
    "customer_id",
    "name",
    "region",
    "signup_date",
    "is_current",
    "effective_from",
    "effective_to"
]

df = pd.DataFrame(data=data, columns=columns)


# Simulate SCD Type 2 Changes
updates = df.sample(int(N * 0.1), random_state=42)  # 10% customers

scd_records = []

for _, row in updates.iterrows():
    change_date = datetime(2026, 1, 1)

    # Close existing record
    scd_records.append([
        row["customer_id"],
        row["name"],
        row["region"],
        row["signup_date"],
        False,
        row["effective_from"],
        change_date.strftime("%Y-%m-%d")
    ])

    # Create new record (region changed)
    scd_records.append([
        row["customer_id"],
        row["name"],
        random.choice(regions),
        row["signup_date"],
        True,
        change_date.strftime("%Y-%m-%d"),
        None
    ])

scd_df = pd.DataFrame(data=scd_records, columns=columns)

df = pd.concat([df, scd_df], ignore_index=True)


# Save file
df.to_csv(os.path.join(output_path, "customers.csv"), index=False)
print("Generated 50K customers + SCD Type 2")
