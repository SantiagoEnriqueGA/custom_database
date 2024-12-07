import sys
import os

# Change the working directory to the parent directory to allow importing the segadb package.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from segadb import *

# Create a new database
db = Database("MyTestDB")

# Create a new table
db.create_table("Users", ["name", "email"])

# Insert a record
users_table = db.get_table("Users")
users_table.try_insert({"name": "John1 Doe", "email": "john@example.com"})
users_table.try_insert({"name": "John2 Doe", "email": "john@example.com"})
users_table.try_insert({"name": "John3 Doe", "email": "john@example.com"})
users_table.try_insert({"name": "John4 Doe", "email": "john@example.com"})
users_table.try_insert({"name": "John5 Doe", "email": "john@example.com"})

print("Before Insert:")
users_table.print_table(pretty=True)

# Modify the record id
users_table.records[0].id = 100

print("\nAfter Changing ID 1:")
users_table.print_table(pretty=True)

# Can insert the record with desired id
users_table.try_insert({"id": 100, "name": "John6 Doe", "email": "john@example.com"})
users_table.try_insert({"id": 1, "name": "John7 Doe", "email": "john@example.com"})

print("\nAfter Insert with ID 1:")
users_table.print_table(pretty=True, index=True)

# Note that the record with id 1 is inserted, as the id was changed
# Note that IDs can be changed, but the index will not be updated