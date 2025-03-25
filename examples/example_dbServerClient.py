import socket
import json

def send_command(host, port, command):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
        client_socket.connect((host, port))
        client_socket.sendall(json.dumps(command).encode('utf-8'))
        
        # Receive the response in chunks
        response = b""
        while True:
            chunk = client_socket.recv(4096)  # Increase buffer size
            if not chunk:
                break
            response += chunk
        
        try:
            return json.loads(response.decode('utf-8'))
        except json.JSONDecodeError as e:
            print(f"JSON decoding error: {e}")
            print(f"Raw response: {response.decode('utf-8')}")
            return {"status": "error", "message": "Invalid JSON response"}

# Example usage
host = '127.0.0.1'
port = 65432

# Insert a record into the "users" table
# ------------------------------------------------------------------------
command = {
    "action": "insert",
    "params": {
        "table": "users",
        "record": {"user_id": 500, "name": "TESTNAME", "email": "test@example.com"}
    }
}
response = send_command(host, port, command)
print(f"\nResponse from insert on users:\n{response}")

# Query the "users" table
# ------------------------------------------------------------------------
command = {
    "action": "query",
    "params": {
        "table": "users"
    }
}
response = send_command(host, port, command)
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
response = send_command(host, port, command)
print(f"\nResponse from query on products with price > 100:\n{response}")

# Query with a filter for category == Electronics
command = {
    "action": "query",
    "params": {
        "table": "products",
        "filter": "lambda record: record.data['category'] == 'Electronics'"
    }
}
response = send_command(host, port, command)
print(f"\nResponse from query on products in Electronics category:\n{response}")

# Stop the database
# ------------------------------------------------------------------------
command = {
    "action": "stop"
}
response = send_command(host, port, command)
print(f"\nResponse from stop command:\n{response}")

# Restart the database
# ------------------------------------------------------------------------
command = {
    "action": "start"
}
response = send_command(host, port, command)
print(f"\nResponse from start command:\n{response}")