import json
import sys
import os
import random
from faker import Faker
import time

# Change the working directory to the parent directory to allow importing the segadb package.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from segadb import *

NUM_RECORDS = 1_000_000

# Create a new database
db = Database("MyTestDB")

columns = ["user_id", "name", "email", "age"]
db.create_table("users", columns)
table = db.get_table("users")


save_dir = 'example_datasets/records.json'
try:
    # Load records from the file
    with open(save_dir, "r") as f:
        records = json.load(f)
except:
    # Create a fake data generator
    fake = Faker()
    # Generate NUM_RECORDS records
    records = [
        {"user_id": i + 1,
        "name": fake.name(),
        "email": fake.email(),
        "age": random.randint(18, 99)
        } for i in range(NUM_RECORDS)
    ]
    # Save records to file
    with open(save_dir, "w") as f:
        json.dump(records, f)

# Insert records
start = time.time()
table.bulk_insert(records) 
end = time.time()
print(f"Inserted {NUM_RECORDS:,} records in {end - start:.2f} seconds")


# Test select time on email column (which is not indexed)
# -----------------------------------------------------------------------------------------------
print(f"\nTesting select on email column (not indexed):")

start = time.time()
selected = table.select(lambda record: "james07@example.net" in record.data["email"])
end = time.time()
print(f"--Select query executed in {end - start:.2f} seconds, found {len(selected)} out of {NUM_RECORDS:,} records")

start = time.time()
filtered = table.filter(lambda record: "john" in record.data["email"]) 
end = time.time()
print(f"--Filter query executed in {end - start:.2f} seconds, found {len(filtered.records)} out of {NUM_RECORDS:,} records")


# Add an index on the email column and test the select time again
# Currently, the select and filter operations only support direct matching on the indexed column.
# This means we can only use the index for exact matches, not for partial matches or complex conditions.
# -----------------------------------------------------------------------------------------------
table.create_index(index_name="email_index", column="email", unique=False)
print(f"\nTesting select on email column (indexed):")

start = time.time()
selected = table.select(index_name="email_index", value="james07@example.net")
end = time.time()
print(f"--Select query executed in {end - start:.2f} seconds, found {len(selected)} out of {NUM_RECORDS:,} records")

start = time.time()
filtered = table.filter(lambda record: "john" in record.data["email"]) 
end = time.time()
print(f"--Filter query executed in {end - start:.2f} seconds, found {len(filtered.records)} out of {NUM_RECORDS:,} records")
