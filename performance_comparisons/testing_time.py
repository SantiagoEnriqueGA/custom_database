import sys
import os
import random
from faker import Faker
from multiprocessing import freeze_support
import time

from sympy import true

# Change the working directory to the parent directory to allow importing the segadb package.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from segadb import *

def insert():
    NUM_RECORDS = 1_000_000

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

    # Insert records in parallel using multiprocessing with custom configuration
    start = time.time()
    table.parallel_insert(records, max_workers=3, chunk_size=1000) 
    end = time.time()
    print(f"Time: {end - start:.2f} seconds: Inserted {NUM_RECORDS:,} records in parallel with custom configuration")
    
    
    # ----------------------------------------------------------------------------------
    # TIME FILTERING
    # ----------------------------------------------------------------------------------
    start = time.time()
    
    print("\nFiltering - Products with price > 100:")
    filtered = db.filter_table("products", lambda record: record.data["price"] > 100)
    
    print("Filtering - Products with category 'Electronics':")
    filtered_electronics = db.filter_table("products", lambda record: record.data["category"] == "Electronics")

    end = time.time()
    print(f"Time: {end - start:.2f} seconds: Filtered {NUM_RECORDS:,} records")
    
    # ----------------------------------------------------------------------------------
    # TIME SORTING
    # ----------------------------------------------------------------------------------
    start = time.time()
    print("\nSorting - Products by price:")
    sorted_products = db.get_table("products").sort("price")
    end = time.time()
    print(f"Time: {end - start:.2f} seconds: Sorted {NUM_RECORDS:,} records")
    
    # ----------------------------------------------------------------------------------
    # TIME JOINING
    # ----------------------------------------------------------------------------------
    print("\nJoining - Products and Products:")
    prod1 = db.get_table("products")
    prod2 = db.get_table("products")
    
    start = time.time()
    prodJoin = prod1.join(other_table=prod2, on_column="product_id", other_column="product_id")
    end = time.time()
    print(f"Time: {end - start:.2f} seconds: Joined {NUM_RECORDS:,} records")
       
    

if __name__ == '__main__':
    freeze_support()
    insert()

