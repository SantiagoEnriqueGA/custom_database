import sys
import os

# Change the working directory to the parent directory to allow importing the segadb package.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from segadb import *
from segadb import databasePartial

# Load and print a sample database
db = Database.load_sample_database(num_users=100, num_orders=200)

# Save the database to a file
Storage.save(db, "example_storage/database_partial.db")

db = databasePartial.PartialDatabase("Partial Database", "example_storage/database_partial.db")

# Tables after initialization
print("Before loading tables:")
db.print_db(index=False, tables=False)

# Only upon calling get_table, the table is loaded into memory
users_table = db.get_table("users")
orders_table = db.get_table("orders")
print("\nAfter loading tables:")
db.print_db(index=False, tables=False)
