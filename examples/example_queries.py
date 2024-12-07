import sys
import os

# Change the working directory to the parent directory to allow importing the segadb package.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from segadb import *

# Create a new database
db = Database("ExampleDB")

# Create two tables: users and orders
db.create_table("users", ["user_id", "name", "email"])
db.create_table("orders", ["user_id", "product", "order_id", "order_date"])

# Add a unique constraint to the users table on the id column
db.add_constraint("users", "user_id", "UNIQUE")

# Add a foreign key constraint to the orders table on the user_id column (user_id in orders must exist in users)
db.add_constraint("orders", "user_id", "FOREIGN_KEY", reference_table_name="users", reference_column="user_id")

# Insert data into the users table
print("Inserting into users table")
db.get_table("users").insert({"user_id": 1, "name": "Alice", "email": "alice@abc.com"})
db.get_table("users").insert({"user_id": 2, "name": "Bob", "email": "bob@abc.com"})
db.get_table("users").insert({"user_id": 3, "name": "Charlie", "email": "charlie@abc.com"})
db.get_table("users").insert({"user_id": 4, "name": "David", "email": "david@abc.com"})

# Insert data into the orders table
print("\nInserting into orders table")
db.get_table("orders").insert({"user_id": 1, "product": "Laptop", "order_id": 1, "order_date": "2021-01-01"})
db.get_table("orders").insert({"user_id": 2, "product": "Phone", "order_id": 2, "order_date": "2021-01-02"})
db.get_table("orders").insert({"user_id": 2, "product": "Laptop", "order_id": 2, "order_date": "2021-01-02"})
db.get_table("orders").insert({"user_id": 2, "product": "Smartwatch", "order_id": 2, "order_date": "2021-01-02"})
db.get_table("orders").insert({"user_id": 3, "product": "Tablet", "order_id": 3, "order_date": "2021-01-03"})
db.get_table("orders").insert({"user_id": 3, "product": "Tablet", "order_id": 3, "order_date": "2021-01-03"})
db.get_table("orders").insert({"user_id": 4, "product": "Smartwatch", "order_id": 4, "order_date": "2021-01-04"})

# Print the tables
print("Users Table:")
db.get_table("users").print_table(pretty=True)

print("\nOrders Table:")
db.get_table("orders").print_table(pretty=True)

# ----------------------------------------------------------------------------------
# JOINS
# ----------------------------------------------------------------------------------
# Perform an inner join between the users and orders tables on the user_id column
print("\nInner Join:")
joined = db.join_tables("users", "orders", "user_id", "user_id")
joined.print_table(pretty=True)

# ----------------------------------------------------------------------------------
# AGGREGATIONS
# ----------------------------------------------------------------------------------
# Aggregate get the count of user_ids
print("\nAggregation - COUNT:")
count = db.get_table("orders").aggregate("user_id", "COUNT")
count.print_table(pretty=True)

# Aggregate get the count of distinct user_ids
print("\nAggregation - COUNT_DISTINCT:")
count = db.get_table("orders").aggregate("user_id", "COUNT_DISTINCT")
count.print_table(pretty=True)

# Aggregate get the minimum user_id
print("\nAggregation - MIN:")
min_order_id = db.get_table("orders").aggregate("user_id", "MIN")
min_order_id.print_table(pretty=True)

# Aggregate get the maximum user_id
print("\nAggregation - MAX:")
max_order_id = db.get_table("orders").aggregate("user_id", "MAX")
max_order_id.print_table(pretty=True)

# Aggregate get the sum of user_id
print("\nAggregation - SUM:")
sum_order_id = db.get_table("orders").aggregate("user_id", "SUM")
sum_order_id.print_table(pretty=True)

# Aggregate get the average user_id
print("\nAggregation - AVG:")
avg_order_id = db.get_table("orders").aggregate("user_id", "AVG")
avg_order_id.print_table(pretty=True)

# ----------------------------------------------------------------------------------
# FILTERING
# ----------------------------------------------------------------------------------
# Filter the orders table where the product is "Laptop"
print("\nFiltering - Laptop Orders:")
filtered = db.filter_table("orders", lambda record: record.data["product"] == "Laptop")
filtered.print_table(pretty=True)

print("\nFiltering - Orders by User 2:")
filtered = db.filter_table("orders", lambda record: record.data["user_id"] == 2)
filtered.print_table(pretty=True)