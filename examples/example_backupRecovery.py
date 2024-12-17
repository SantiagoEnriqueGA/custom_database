import sys
import os

# Change the working directory to the parent directory to allow importing the segadb package.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from segadb import *

# Create a new database
db = Database("ExampleDB")
user_manager = db.create_user_manager()
auth = db.create_authorization()

# Register users with different roles
user_manager.register_user("admin", "password123", roles=["admin"])
user_manager.register_user("user1", "password123", roles=["read_only"])

# Log in as admin
admin_session = user_manager.login_user("admin", "password123")

# Create two tables: users and orders
db.create_table("users", ["user_id", "name", "email"])
db.create_table("orders", ["user_id", "product", "order_id", "order_date"])

# Add a unique constraint to the users table on the id column
db.add_constraint("users", "user_id", "UNIQUE")

# Add a foreign key constraint to the orders table on the user_id column (user_id in orders must exist in users)
db.add_constraint("orders", "user_id", "FOREIGN_KEY", reference_table_name="users", reference_column="user_id")

# Insert data into the users table
db.get_table("users").insert({"user_id": 1, "name": "Alice", "email": "alice@abc.com"})
db.get_table("users").insert({"user_id": 2, "name": "Bob", "email": "bob@abc.com"})
db.get_table("users").insert({"user_id": 3, "name": "Charlie", "email": "charlie@abc.com"})
db.get_table("users").insert({"user_id": 4, "name": "David", "email": "david@abc.com"})

# Insert data into the orders table
db.get_table("orders").insert({"user_id": 1, "product": "Laptop", "order_id": 1, "order_date": "2021-01-01"})
db.get_table("orders").insert({"user_id": 2, "product": "Phone", "order_id": 2, "order_date": "2021-01-02"})
db.get_table("orders").insert({"user_id": 2, "product": "Laptop", "order_id": 2, "order_date": "2021-01-02"})
db.get_table("orders").insert({"user_id": 2, "product": "Smartwatch", "order_id": 2, "order_date": "2021-01-02"})
db.get_table("orders").insert({"user_id": 3, "product": "Tablet", "order_id": 3, "order_date": "2021-01-03"})
db.get_table("orders").insert({"user_id": 3, "product": "Tablet", "order_id": 3, "order_date": "2021-01-03"})
db.get_table("orders").insert({"user_id": 4, "product": "Smartwatch", "order_id": 4, "order_date": "2021-01-04"})

# Print the tables before saving
print("Before Saving:")
print("-" * 100)
db.print_db()


# Create multiple backups of the database
Storage.backup(db, dir="example_storage/example_backups", date=True)
Storage.backup(db, dir="example_storage/example_backups", date=True)
Storage.backup(db, dir="example_storage/example_backups", date=True)
Storage.backup(db, dir="example_storage/example_backups", date=True)

# Show the backups
print("\nBackups:")
print("-" * 100)
Storage.list_backups("example_storage/example_backups")

# Drop a table
db.drop_table("orders")

# Print the tables after dropping the orders table
print("\nAfter Dropping the 'orders' Table:")
print("-" * 100)
db.print_db()


# Restore the database from a backup
db = Storage.restore(db,
                     user="admin", password="password123",
                     dir="example_storage/example_backups",
                     backup_name="backup_1_")

# Print the tables after restoring from a backup
print("\nAfter Restoring from Backup:")
print("-" * 100)
db.print_db()