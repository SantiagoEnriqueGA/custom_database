import sys
import os
import json

# Change the working directory to the parent directory to allow importing the segadb package.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from segadb import *

# Initialize the SocketClient
socket = SocketClient(host='127.0.0.1', port=65432)

def print_header(text):
    text_len = len(text)
    print("-" *  100)
    print(" " * (50 - text_len//2) + text + " " * (50 - text_len//2))
    print("-" *  100)

# --- Server Control ---
print_header("Server Control")
# ------------------------------------------------------------------------
# Ping the database
command = {
    "action": "ping"
}
response = socket.send_command(command)
print(f"Response from ping command:\n{response}")

# Stop the database
command = {
    "action": "stop"
}
response = socket.send_command(command)
print(f"\nResponse from stop command:\n{response}")

# Restart the database
command = {
    "action": "start"
}
response = socket.send_command(command)
print(f"\nResponse from start command:\n{response}")




# --- Authentication ---
print_header("Authentication")
# ------------------------------------------------------------------------
# Register User
command = {
    "action": "register_user",
    "params": {
        "username": "newuser",
        "password": "password123",
        "roles": ["admin"]
    }
}
response = socket.send_command(command)
print(f"Response from register_user command:\n{response}")

# Login User
command = {
    "action": "login_user",
    "params": {
        "username": "newuser",
        "password": "password123"
    }
}
response = socket.send_command(command)
print(f"\nResponse from login_user command:\n{response}")
# Extract session token
session_token = response['session_token']

# Logout User
print_header("Logout User")
command = {
    "action": "logout_user",
    "params": {
        "session_token": session_token
    }
}
response = socket.send_command(command)
print(f"\nResponse from logout_user command:\n{response}")



# --- Table Management ---
print_header("Table Management")
# ------------------------------------------------------------------------
# Create Table
command = {
    "action": "create_table",
    "params": {
        "table_name": "employees",
        "columns": ["employee_id", "name", "department"]
    }
}
response = socket.send_command(command)
print(f"Response from create_table command:\n{response}")

# List Tables
command = {
    "action": "list_tables"
}
response = socket.send_command(command)
print(f"\nResponse from list_tables command:\n{response}")

# Drop Table
command = {
    "action": "drop_table",
    "params": {
        "table_name": "employees"
    }
}
response = socket.send_command(command)
print(f"\nResponse from drop_table command:\n{response}")

# Create a "products" table
command = {
    "action": "create_table",
    "params": {
        "table_name": "products",
        "columns": ["product_id", "name", "category", "price"]
    }
}
response = socket.send_command(command)
print(f"\nResponse from create_table command:\n{response}")

# Insert some sample data into the products table
command = {
    "action": "insert",
    "params": {
        "table": "products",
        "record": {"product_id": 1, "name": "Laptop", "category": "Electronics", "price": 1200}
    }
}
response = socket.send_command(command)
print(f"\nResponse from insert command:\n{response}")

command = {
    "action": "insert",
    "params": {
        "table": "products",
        "record": {"product_id": 2, "name": "T-Shirt", "category": "Clothing", "price": 25}
    }
}
response = socket.send_command(command)
print(f"\nResponse from insert command:\n{response}")



# --- Record Operations ---
print_header("Record Operations")
# ------------------------------------------------------------------------
# Insert
command = {
    "action": "insert",
    "params": {
        "table": "users",
        "record": {"user_id": 500, "name": "TESTNAME", "email": "test@example.com"}
    }
}
response = socket.send_command(command)
print(f"Response from insert on users:\n{response}")

# Query
command = {
    "action": "query",
    "params": {
        "table": "users"
    }
}
response = socket.send_command(command)
print(f"\nResponse from query on users:")
if response.get("status") == "success":
    print(response.get("status"))
    print("Data:")
    for record in response.get("data"):
        print(f"\t{record.get('user_id')} | {record.get('name')} | {record.get('email')}")
else:
    print(response)

# Query with Filters
# Query with a filter for price > 100
command = {
    "action": "query",
    "params": {
        "table": "products",
        "filter": "lambda record: record.data['price'] > 100"
    }
}
response = socket.send_command(command)
print(f"\nResponse from query on products with price > 100:\n{response}")

# Query with a filter for category == Electronics
command = {
    "action": "query",
    "params": {
        "table": "products",
        "filter": "lambda record: record.data['category'] == 'Electronics'"
    }
}
response = socket.send_command(command)
print(f"\nResponse from query on products in Electronics category:\n{response}")

# Update
command = {
    "action": "update",
    "params": {
        "table": "products",
        "record_id": 1,
        "updates": {"price": 1250}
    }
}
response = socket.send_command(command)
print(f"\nResponse from update command:\n{response}")

# Delete
command = {
    "action": "delete",
    "params": {
        "table": "users",
        "record_id": 500
    }
}
response = socket.send_command(command)
print(f"\nResponse from delete command:\n{response}")



# --- Stored Procedure Execution ---
print_header("Stored Procedure Execution")
# ------------------------------------------------------------------------
# Execute a stored procedure
command = {
    "action": "execute_procedure",
    "params": {
        "procedure_name": "get_orders_by_user",
        "procedure_params": {"user_id": 3}
    }
}
response = socket.send_command(command)
print(f"Response from execute_procedure command:\n{response}")

# Add a stored procedure
command = {
    "action": "create_procedure",
    "params": {
        "procedure_name": "get_users_by_name_starting_with",
        "procedure_code": "    return db.filter_table('users', lambda record: record.data['name'].startswith(name))"
    }
}
response = socket.send_command(command)
print(f"\nResponse from create_procedure command:\n{response}")

# Execute the new stored procedure
command = {
    "action": "execute_procedure",
    "params": {
        "procedure_name": "get_users_by_name_starting_with",
        "procedure_params": {"name": "Sa"}
    }
}
response = socket.send_command(command)
print(f"\nResponse from execute_procedure command:\n{response}")



# --- Cleanup ---
print_header("Cleanup")
# Cleanup - Drop the products table
# ------------------------------------------------------------------------
command = {
    "action": "drop_table",
    "params": {
        "table_name": "products"
    }
}
response = socket.send_command(command)
print(f"Response from drop_table command:\n{response}")

# Cleanup - Drop the users table
# ------------------------------------------------------------------------
command = {
    "action": "drop_table",
    "params": {
        "table_name": "users"
    }
}
response = socket.send_command(command)
print(f"\nResponse from drop_table command:\n{response}")