import sys
import os

# Change the working directory to the parent directory to allow importing the segadb package.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from segadb import *

# Create a new database with a user manager and authorization
db = Database("MyTestDB")
user_manager = db.create_user_manager()
auth = db.create_authorization()

# Register users
# --------------------------------------------------------------------------------
user_manager.register_user("admin", "password123", roles=["admin"])
user_manager.register_user("user1", "password123", roles=["read_only"])
user_manager.register_user("editor", "password123", roles=["read_only", "editor"])
admin_session = user_manager.login_user("admin", "password123")

# Create a new table from a CSV file
# ----------------------------------------------------------------------------------
db.create_table_from_csv("example_datasets/pima-indians-diabetes.csv", "WBP", headers=True)

# Create a new base tables (users and orders) with foreign key constraints
# ----------------------------------------------------------------------------------
# Create two tables: users and orders
db.create_table("users", ["user_id", "name", "email"])
users = db.get_table("users")
db.create_table("orders", ["user_id", "product", "order_id", "order_date"])
orders = db.get_table("orders")

# Add a unique constraint to the users table on the id column
db.add_constraint("users", "user_id", "UNIQUE")
db.add_constraint("users", "email", lambda x: "@" in x)

# Add a foreign key constraint to the orders table on the user_id column (user_id in orders must exist in users)
db.add_constraint("orders", "user_id", "FOREIGN_KEY", reference_table_name="users", reference_column="user_id")

# Insert data into the users table
users.insert({"user_id": 1, "name": "Alice", "email": "alice@abc.com"})
users.insert({"user_id": 2, "name": "Bob", "email": "bob@abc.com"})
users.insert({"user_id": 3, "name": "Charlie", "email": "charlie@abc.com"})
users.insert({"user_id": 4, "name": "David", "email": "david@abc.com"})

# Insert data into the orders table
orders.insert({"user_id": 1, "product": "Laptop", "order_id": 1, "order_date": "2021-01-01"})
orders.insert({"user_id": 2, "product": "Phone", "order_id": 2, "order_date": "2021-01-02"})
orders.insert({"user_id": 3, "product": "Tablet", "order_id": 3, "order_date": "2021-01-03"})
orders.insert({"user_id": 4, "product": "Smartwatch", "order_id": 4, "order_date": "2021-01-04"})

# Create a new table for VectorRecords
# ----------------------------------------------------------------------------------
db.create_table("VectorRecords", ["vector"])
vectorRecords = db.get_table("VectorRecords")
vectorRecords.insert({"vector": [1.0, 2.0, 3.0]}, record_type=VectorRecord)
vectorRecords.insert({"vector": [4.0, 5.0, 6.0]}, record_type=VectorRecord)
vectorRecords.insert({"vector": [7.0, 8.0, 9.0]}, record_type=VectorRecord)
vectorRecords.insert({"vector": [5.0, 5.0, 5.0]}, record_type=VectorRecord)

# Create a new table for TimeSeriesRecords
# ----------------------------------------------------------------------------------
db.create_table("TimeSeriesRecords", ["time_series"])
timeSeriesRecords = db.get_table("TimeSeriesRecords")
timeSeriesRecords.insert({"time_series": [1, 2, 3, 4]}, record_type=TimeSeriesRecord)
timeSeriesRecords.insert({"time_series": [5, 6, 7, 8]}, record_type=TimeSeriesRecord)
timeSeriesRecords.insert({"time_series": [9, 10, 11, 12]}, record_type=TimeSeriesRecord)

# Create a new table for TextRecords
# ----------------------------------------------------------------------------------
db.create_table("TextRecords", ["text"])
textRecords = db.get_table("TextRecords")
textRecords.insert({"text": "Hello, world!"}, record_type=TextRecord)
textRecords.insert({"text": "Goodbye, world!"}, record_type=TextRecord)
textRecords.insert({"text": "Hello, again!"}, record_type=TextRecord)


# Create a new table for ImageRecords
# ----------------------------------------------------------------------------------
db.create_table("ImageRecords", ["image_data"])
imageRecords = db.get_table("ImageRecords")
imageRecords.insert({"image_data": 'example_datasets/cube.png'}, record_type=ImageRecord)
imageRecords.insert({"image_data": 'example_datasets/cube.png'}, record_type=ImageRecord)
imageRecords.insert({"image_data": 'example_datasets/cube.png'}, record_type=ImageRecord)

# Print the database details
# ----------------------------------------------------------------------------------
db.print_db(limit=5)


# Save the database to an encrypted file
# ----------------------------------------------------------------------------------
# Generate a random encryption key and save the database to an encrypted file
key = Storage.generate_key()
print(f"Encryption Key: {key}")
Storage.save(db, "example_storage/database_encrypted.segadb", key=key)

# Show a preview of encrypted file
with open("example_storage/database_encrypted.segadb", "r") as f:
    print(f"Encrypted File Preview: {f.read(100)}")


# Load the database from an encrypted file
# ----------------------------------------------------------------------------------
loaded_db = Storage.load("example_storage/database_encrypted.segadb", key=key, user="admin", password="password123")
db.print_db(limit=5)
