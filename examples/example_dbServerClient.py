import sys
import os

# Change the working directory to the parent directory to allow importing the segadb package.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from segadb import *

# Initialize the SocketClient
socket = SocketClient(host='127.0.0.1', port=65432)

# Insert a record into the "users" table
# ------------------------------------------------------------------------
command = {
    "action": "insert",
    "params": {
        "table": "users",
        "record": {"user_id": 500, "name": "TESTNAME", "email": "test@example.com"}
    }
}
response = socket.send_command(command)
print(f"\nResponse from insert on users:\n{response}")

# Query the "users" table
# ------------------------------------------------------------------------
command = {
    "action": "query",
    "params": {
        "table": "users"
    }
}
response = socket.send_command(command)
print(f"\nResponse from query on users:\n{response}")

# Query with a filters
# ------------------------------------------------------------------------
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

# Stop the database
# ------------------------------------------------------------------------
command = {
    "action": "stop"
}
response = socket.send_command(command)
print(f"\nResponse from stop command:\n{response}")

# Restart the database
# ------------------------------------------------------------------------
command = {
    "action": "start"
}
response = socket.send_command(command)
print(f"\nResponse from start command:\n{response}")