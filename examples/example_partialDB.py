import sys
import os

# Change the working directory to the parent directory to allow importing the segadb package.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from segadb import *

# Load and print a sample database
db = Database.load_sample_database(n_users=3, n_orders=3)

# Save the database to a file
Storage.save(db, "example_storage/database_partial.db")

# Load the database from the file (without views, materialized views, stored procedures, and triggers as they will load their respective tables)
db = PartialDatabase("Partial Database", "example_storage/database_partial.db",
                        views=False, materialized_views=False, stored_procedures=False, triggers=False)

# Tables after initialization
print("Before loading tables:")
# db.print_db(index=False, tables=False)
print("Active tables:", db.active_tables())


# Only upon calling get_table, the table is loaded into memory
users_table = db.get_table("users")
orders_table = db.get_table("orders")

print("\nAfter loading tables:")
# db.print_db(index=False, tables=False)
print("Active tables:", db.active_tables())

# Insert a new user
users_table.insert({"user_id": 999, "name": "Bobby", "email": "bobby@abc.com"})
print("\nUsers Table:")
users_table.print_table(pretty=True, limit=5)

# Deactivate the users table (will save the table to file)
db.deactivate_table("users")

print("\nAfter deactivating users table:")
print("Active tables:", db.active_tables())

# Load the users table again
users_table = db.get_table("users")
print("\nAfter loading users table:")
print("Active tables:", db.active_tables())
users_table.print_table(pretty=True, limit=5)