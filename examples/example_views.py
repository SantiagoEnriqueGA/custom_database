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
db.get_table("orders").insert({"user_id": 2, "product": "Laptop", "order_id": 9, "order_date": "2021-01-02"})
db.get_table("orders").insert({"user_id": 2, "product": "Smartwatch", "order_id": 2, "order_date": "2021-01-02"})
db.get_table("orders").insert({"user_id": 3, "product": "Tablet", "order_id": 3, "order_date": "2021-01-03"})
db.get_table("orders").insert({"user_id": 3, "product": "Tablet", "order_id": 12, "order_date": "2021-01-03"})
db.get_table("orders").insert({"user_id": 4, "product": "Smartwatch", "order_id": 8, "order_date": "2021-01-04"})

# Print the tables
print("Users Table:")
db.get_table("users").print_table(pretty=True)

print("\nOrders Table:")
db.get_table("orders").print_table(pretty=True)


# Example usage, create a view of laptops
# ----------------------------------------------------------------------------------
def laptop_view():
    return db.filter_table("orders", lambda record: record.data["product"] == "Laptop")

db.create_view("laptop_view", laptop_view)

# Retrieve the data for the laptop view
v_laptopOrders = db.get_view("laptop_view")
print("\nLaptop View:")
v_laptopOrders.get_data().print_table(pretty=True)

# Example usage, create a view of users with orders
# ----------------------------------------------------------------------------------
def users_with_orders_view():
    return db.join_tables("users", "orders", "user_id", "user_id")

db.create_view("users_with_orders_view", users_with_orders_view)

# Retrieve the data for the users with orders view
v_usersWtOrders = db.get_view("users_with_orders_view")
print("\nUsers with Orders View:")
v_usersWtOrders.get_data().print_table(pretty=True)


# Example usage, create a materialized view of Orders by User 2
# ----------------------------------------------------------------------------------
def mv_ordersUser2():
    return db.filter_table("orders", lambda record: record.data["user_id"] == 2)

db.create_materialized_view("mv_ordersUser2", mv_ordersUser2)

# Retrieve the data for the materialized view
mv_ordersUser2 = db.get_materialized_view("mv_ordersUser2")
print("\nMaterialized View - Orders by User 2:")
mv_ordersUser2.get_data().print_table(pretty=True)