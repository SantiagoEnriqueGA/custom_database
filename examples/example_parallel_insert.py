import sys
import os
import random
from faker import Faker
from multiprocessing import freeze_support

# Change the working directory to the parent directory to allow importing the segadb package.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from segadb import *

def insert():
    NUM_RECORDS = 10000

    # Create a new database
    db = Database("MyTestDB")
    
    columns = ["product_id", "name", "category", "price"]
    db.create_table("products", columns)
    table = db.get_table("products")

    # Create a fake data generator
    fake = Faker()

    # Generate NUM_RECORDS records
    records = [
        {"product_id": i + 1, 
        "name": fake.word(), 
        "category": random.choice(["Electronics", "Clothing", "Home", "Sports"]), "price": random.uniform(10.0, 500.0)
        } for i in range(NUM_RECORDS)
    ]

    # Insert records in parallel using multiprocessing
    table.parallel_insert(records)
    print(f"Inserted {NUM_RECORDS} records in parallel")

    # Insert records in parallel using multiprocessing with custom configuration
    table.parallel_insert(records, max_workers=3, chunk_size=1000) 
    print(f"Inserted {NUM_RECORDS} records in parallel with custom configuration")

    # Insert records in parallel using safe version of insert
    table.parallel_try_insert(records)
    print(f"Inserted {NUM_RECORDS} records in parallel using safe version of insert")


if __name__ == '__main__':
    freeze_support()
    insert()
