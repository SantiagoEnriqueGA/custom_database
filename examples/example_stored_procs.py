from json import load
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

# Add a unique constraint to the users t;able on the id column
db.add_constraint("users", "user_id", "UNIQUE")

# Add a foreign key constraint to the orders table on the user_id column (user_id in orders must exist in users)
db.add_constraint("orders", "user_id", "FOREIGN_KEY", reference_table_name="users", reference_column="user_id")

# Insert data into the users table
db.get_table("users").insert({"user_id": 1, "name": "Alice", "email": "alice@abc.com"})
db.get_table("users").insert({"user_id": 2, "name": "Bob", "email": "bob@abc.com"})
db.get_table("users").insert({"user_id": 3, "name": "Charlie", "email": "charlie@abc.com"})
db.get_table("users").insert({"user_id": 4, "name": "David", "email": "david@abc.com"})
db.get_table("users").insert({"user_id": 5, "name": "Brad5", "email": "brad@abc.com"})
db.get_table("users").insert({"user_id": 6, "name": "Brad6", "email": "brad@abc.com"})
db.get_table("users").insert({"user_id": 7, "name": "Brad7", "email": "brad@abc.com"})
db.get_table("users").insert({"user_id": 8, "name": "Brad8", "email": "brad@abc.com"})

# Insert data into the orders table
db.get_table("orders").insert({"user_id": 1, "product": "Laptop", "order_id": 1, "order_date": "2021-01-01"})
db.get_table("orders").insert({"user_id": 2, "product": "Phone", "order_id": 2, "order_date": "2021-01-02"})
db.get_table("orders").insert({"user_id": 2, "product": "Laptop", "order_id": 2, "order_date": "2021-01-02"})
db.get_table("orders").insert({"user_id": 2, "product": "Smartwatch", "order_id": 2, "order_date": "2021-01-02"})
db.get_table("orders").insert({"user_id": 3, "product": "Tablet", "order_id": 3, "order_date": "2021-01-03"})
db.get_table("orders").insert({"user_id": 3, "product": "Tablet", "order_id": 3, "order_date": "2021-01-03"})
db.get_table("orders").insert({"user_id": 4, "product": "Smartwatch", "order_id": 4, "order_date": "2021-01-04"})

# db.print_db(views=True, materialized_views=True)


# Define a stored procedure to get orders by user
# ----------------------------------------------------------------------------------
def get_orders_by_user(db, user_id):
    """
    Retrieve orders for a specific user.
    Args:
        db (Database): The database instance.
        user_id (int): The ID of the user.
    Returns:
        list: A list of orders for the specified user.
    """
    return db.filter_table("orders", lambda record: record.data["user_id"] == user_id)

# Add the stored procedure to the database
db.add_stored_procedure("get_orders_by_user", get_orders_by_user)

# Execute the stored procedure
orders_by_user_2 = db.execute_stored_procedure("get_orders_by_user", 2)

print("\n\nOrders for User 2:")
orders_by_user_2.print_table(pretty=True)


# Define a stored procedure to drop users with no orders
# ----------------------------------------------------------------------------------
def drop_users_with_no_orders(db):
    """
    Drop users who have no orders.
    Args:
        db (Database): The database instance.
    """
    orders = db.get_table("orders")
    users = db.get_table("users")
    users_to_drop = set()
    for user in users.records:
        user_orders = orders.filter(lambda record: record.data["user_id"] == user.data["user_id"])
        if len(user_orders.records) == 0:
            users_to_drop.add(user.data["user_id"])
    
    print(f"\nDropping users with no orders:")
    for user_id in users_to_drop:
        print(f"\tDropping user with ID: {user_id}")
        users.delete(user_id)
        
# Add the stored procedure to the database
db.add_stored_procedure("drop_users_with_no_orders", drop_users_with_no_orders)

print("\n\nUsers before dropping users with no orders:")
db.get_table("users").print_table(pretty=True)

# Execute the stored procedure
db.execute_stored_procedure("drop_users_with_no_orders")

print("\nUsers after dropping users with no orders:")
db.get_table("users").print_table(pretty=True)


# Save the database to a file
Storage.save(db, "example_storage/database.segadb")

# Load the database from a file
loaded_db = Storage.load("example_storage/database.segadb", user="admin", password="password123")

# Print the tables after loading
print("\nAfter Loading:")
print("--------------------------------------------------------------------------------")

db.print_db(views=True, materialized_views=True, tables=False)
loaded_db.print_db(views=True, materialized_views=True, tables=False)

# Execute the stored procedure from the loaded database
loaded_db.execute_stored_procedure("get_orders_by_user", 2).print_table(pretty=True)
