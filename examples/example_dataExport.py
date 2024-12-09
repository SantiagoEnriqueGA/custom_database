import sys
import os
import random

# Change the working directory to the parent directory to allow importing the segadb package.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from segadb import *

# Create a new database
db = Database("ExampleDB")

# Create a new table called users with columns user_id, name, and email
db.create_table("users", ["user_id", "name", "email"])

# Add a unique constraint to the users table on the id column
db.add_constraint("users", "user_id", "UNIQUE")

# Insert data into the users table with randomized 6-digit user_ids
print("Inserting into users table")
user_ids = random.sample(range(100000, 999999), 14)
db.get_table("users").insert({"user_id": user_ids[0], "name": "Alice", "email": "alice@abc.com"})
db.get_table("users").insert({"user_id": user_ids[1], "name": "Bob", "email": "bob@abc.com"})
db.get_table("users").insert({"user_id": user_ids[2], "name": "Charlie", "email": "charlie@abc.com"})
db.get_table("users").insert({"user_id": user_ids[3], "name": "David", "email": "david@abc.com"})
db.get_table("users").insert({"user_id": user_ids[4], "name": "Eve", "email": "eve@abc.com"})
db.get_table("users").insert({"user_id": user_ids[5], "name": "Frank", "email": "frank@abc.com"})
db.get_table("users").insert({"user_id": user_ids[6], "name": "Grace", "email": "grace@abc.com"})
db.get_table("users").insert({"user_id": user_ids[7], "name": "Hank", "email": "hank@abc.com"})
db.get_table("users").insert({"user_id": user_ids[8], "name": "Ivy", "email": "ivy@abc.com"})
db.get_table("users").insert({"user_id": user_ids[9], "name": "Jack", "email": "jack@abc.com"})
db.get_table("users").insert({"user_id": user_ids[10], "name": "Karen", "email": "karen@abc.com"})
db.get_table("users").insert({"user_id": user_ids[11], "name": "Leo", "email": "leo@abc.com"})
db.get_table("users").insert({"user_id": user_ids[12], "name": "Mona", "email": "mona@abc.com"})
db.get_table("users").insert({"user_id": user_ids[13], "name": "Nina", "email": "nina@abc.com"})

# Print the data in the users table
print("\nPrinting users table")
db.get_table("users").print_table(pretty=True, index=True)

# Save the table to a csv
print("\nSaving users table to CSV")
Storage.save_table(db.get_table("users"), "example_storage/users.csv", format="csv")

# Save the table to a json
print("\nSaving users table to JSON")
Storage.save_table(db.get_table("users"), "example_storage/users.json", format="json")

# Save the table to a SQLITE database file
print("\nSaving users table to SQLite database")
Storage.save_table(db.get_table("users"), "example_storage/users.db", format="sqlite")