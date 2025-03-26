import sys
import os
import json

# Change the working directory to the parent directory to allow importing the segadb package.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from segadb import *

def print_header(text):
    text_len = len(text)
    print("-" *  100)
    print(" " * (50 - text_len//2) + text + " " * (50 - text_len//2))
    print("-" *  100)
    
# Initialize the SocketClient
socket = SocketClient(host='127.0.0.1', port=65432)

# --- Server Control ---
print_header("Server Control")
# ------------------------------------------------------------------------
# Ping the database
response = socket.ping()
print(f"Response from ping command:\n{response}")

# Stop the database
response = socket.stop()
print(f"\nResponse from stop command:\n{response}")

# Restart the database
response = socket.start()
print(f"\nResponse from start command:\n{response}")



# --- Authentication ---
print_header("Authentication")
# ------------------------------------------------------------------------
# Register User
response = socket.register_user(username="newuser", password="password123", roles=["admin"])
print(f"Response from register_user command:\n{response}")

# Login User
response = socket.login_user(username="newuser", password="password123")
print(f"\nResponse from login_user command:\n{response}")
# Extract session token
session_token = response['session_token']

# Logout User
print_header("Logout User")
response = socket.logout_user(session_token=session_token)
print(f"\nResponse from logout_user command:\n{response}")



# --- Table Management ---
print_header("Table Management")
# ------------------------------------------------------------------------
# Create Table
response = socket.create_table(table_name="employees", columns=["employee_id", "name", "department"])
print(f"Response from create_table command:\n{response}")

# List Tables
response = socket.list_tables()
print(f"\nResponse from list_tables command:\n{response}")

# Drop Table
response = socket.drop_table(table_name="employees")
print(f"\nResponse from drop_table command:\n{response}")

# Create a "products" table
response = socket.create_table(table_name="products", columns=["product_id", "name", "category", "price"])
print(f"\nResponse from create_table command:\n{response}")

# Insert some sample data into the products table
response = socket.insert_record(table="products", record={"product_id": 1, "name": "Laptop", "category": "Electronics", "price": 1200})
print(f"\nResponse from insert command:\n{response}")

response = socket.insert_record(table="products", record={"product_id": 2, "name": "T-Shirt", "category": "Clothing", "price": 25})
print(f"\nResponse from insert command:\n{response}")



# --- Record Operations ---
print_header("Record Operations")
# ------------------------------------------------------------------------
# Insert
response = socket.insert_record(table="users", record={"user_id": 50000, "name": "TESTNAME", "email": "test@example.com"})
print(f"Response from insert on users:\n{response}")

# Query
response = socket.query_table(table="users")
print(f"\nResponse from query on users:")
print(f"Status: {response.get('status')}")
print("Data:")
SocketUtilities.print_results(response.get("data"), response.get("columns"))

# Query with Filters
# Query with a filter for price > 100
response = socket.query_table(table="products", filter="lambda record: record.data['price'] > 100")
print(f"\nResponse from query on products with price > 100:\n{response}")

# Query with a filter for category == Electronics
response = socket.query_table(table="products", filter="lambda record: record.data['category'] == 'Electronics'")
print(f"\nResponse from query on products in Electronics category:\n{response}")

# Update
response = socket.update_record(table="products", record_id=1, updates={"price": 1250})
print(f"\nResponse from update command:\n{response}")

# Delete
response = socket.delete_record(table="users", record_id=500)
print(f"\nResponse from delete command:\n{response}")



# --- Stored Procedure Execution ---
print_header("Stored Procedure Execution")
# ------------------------------------------------------------------------
# Execute a stored procedure
response = socket.execute_procedure(procedure_name="get_orders_by_user", procedure_params={"user_id": 3})
print(f"Response from execute_procedure command:")
print(f"Status: {response.get('status')}")
print("Data:")
SocketUtilities.print_results(response.get("data"), response.get("columns"))

# Add a stored procedure
response = socket.create_procedure(procedure_name="get_users_by_name_starting_with", procedure_code="name = kwargs.get('name'); return db.filter_table('users', lambda record: record.data['name'].startswith(name))")
print(f"\nResponse from create_procedure command:\n{response}")

# Execute the new stored procedure
response = socket.execute_procedure(procedure_name="get_users_by_name_starting_with", procedure_params={"name": "Sa"})
print(f"\nResponse from query on users:")
print(f"Status: {response.get('status')}")
print("Data:")
SocketUtilities.print_results(response.get("data"), response.get("columns"))


# --- Database Information ---
print_header("Database Information")
# ------------------------------------------------------------------------
# Get Database Information
response = socket.get_db_info()
print(f"Response from get_db_info command:")
print(f"Status: {response.get('status')}")
if response.get('status') == 'success':
    for key, value in response.get('data', {}).items():
        print(f"\t{key:25}: {value}")


# --- Views ---
print_header("Views")
# ------------------------------------------------------------------------
# List Views
response = socket.list_views()
print(f"Response from list_views command:\n{response}")

# Query a View
response = socket.query_view(view_name="laptop_view")
print(f"\nResponse from query on laptop_view:")
print(f"Status: {response.get('status')}")
print("Data:")
SocketUtilities.print_results(response.get("data"), response.get("columns"))

# Create a View
response = socket.create_view(view_name="phone_view", query_code="return db.filter_table('orders', lambda record: record.data['product'] == 'Phone')")
print(f"\nResponse from create_view command:\n{response}")

# Drop a View
response = socket.drop_view(view_name="phone_view")
print(f"\nResponse from drop_view command:\n{response}")



# --- Materialized Views ---
print_header("Materialized Views")
# ------------------------------------------------------------------------
# List Materialized Views
response = socket.list_materialized_views()
print(f"Response from list_materialized_views command:\n{response}")

# Query a Materialized View
response = socket.query_materialized_view(view_name="mv_ordersUserEven")
print(f"\nResponse from query on mv_ordersUserEven:")
print(f"Status: {response.get('status')}")
print("Data:")
SocketUtilities.print_results(response.get("data"), response.get("columns"))

# Create a Materialized View
response = socket.create_materialized_view(view_name="phone_view", query_code="return db.filter_table('orders', lambda record: record.data['product'] == 'Phone')")
print(f"\nResponse from create_materialized_view command:\n{response}")

# Drop a Materialized View
response = socket.drop_materialized_view(view_name="phone_view")
print(f"\nResponse from drop_materialized_view command:\n{response}")



