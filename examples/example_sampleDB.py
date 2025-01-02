import sys
import os

# Change the working directory to the parent directory to allow importing the segadb package.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from segadb import *

# Load and print a sample database
db = Database.load_sample_database(n_users=1000, n_orders=10_000, n_products=50, n_reviews=200, n_categories=10, n_suppliers=20)
db.print_db(index=False, limit=10, tables=True, 
            views=True, materialized_views=True,
            stored_procedures=True, triggers=True)

# Execute a stored procedure
len_before = len(db.get_table('users').records)
db.execute_stored_procedure("drop_users_with_no_orders")

print("\n\nUsers BEFORE dropping users with no orders:")
print(f"Table length: {len_before}")

print("\nUsers AFTER dropping users with no orders:")
print(f"Table length: {len(db.get_table('users').records)}")
