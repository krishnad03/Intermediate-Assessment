import json
import random
import os
from datetime import datetime, timedelta


# Basic Settings

total_records = 100000
batch_size = 5000
regions = ["North", "South", "East", "West", "Central"]

start_time = datetime(2026, 1, 1)

# Create streaming folder

os.makedirs("../data/streaming", exist_ok=True)

record_number = 0
batch_number = 1


# Generate batches

while record_number < total_records:

    batch_data = []

    for _ in range(batch_size):

        event_time = start_time + timedelta(seconds=record_number)

        # 5% late records
        if random.random() < 0.05:
            event_time = event_time - timedelta(minutes=random.randint(5, 20))

        record = {
            "event_time": event_time.isoformat(),
            "customer_id": random.randint(1, 50000),
            "region": random.choice(regions),
            "amount": round(random.uniform(100, 5000), 2)
        }

        batch_data.append(record)
        record_number += 1

    file_path = f"../data/streaming/batch_{batch_number}.json"

    with open(file_path, "w") as f:
        json.dump(batch_data, f, indent=2)

    batch_number += 1

print("Generated streaming batch files successfully.")
