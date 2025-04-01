import socket
import json
import time
class SocketClient:
    """
    A helper class to manage socket connections and communication.
    """
    def __init__(self, host='127.0.0.1', port=65432):
        """
        Initialize the SocketHelper with the host and port.

        Args:
            host (str): The server's hostname or IP address.
            port (int): The port number to connect to.
        """
        self.host = host
        self.port = port

    def send_command(self, command):
        """
        Send a command to the server and receive the response.

        Args:
            command (dict): The command to send, in dictionary format.

        Returns:
            dict: The server's response, parsed as JSON.
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
            try:
                client_socket.connect((self.host, self.port))
                client_socket.sendall(json.dumps(command).encode('utf-8'))

                # Receive the response in chunks
                response = b""
                while True:
                    chunk = client_socket.recv(4096)
                    if not chunk:
                        break
                    response += chunk

                return json.loads(response.decode('utf-8'))
            except json.JSONDecodeError as e:
                print(f"JSON decoding error: {e}")
                print(f"Raw response: {response.decode('utf-8')}")
                return {"status": "error", "message": "Invalid JSON response"}
            except Exception as e:
                # print(f"Socket error: {e}")
                return {"status": "error", "message": str(e)}   
    
    # --- Server Control ---
    def ping(self):
        """
        Ping the server to check if it is running and measure response time.
        """
        command = {
            "action": "ping"
        }
        start_time = time.time()
        response = self.send_command(command)
        end_time = time.time()
        response["response_time"] = end_time - start_time
        return response
    
    def stop(self):
        """
        Stop the database server.
        """
        command = {
            "action": "stop"
        }
        return self.send_command(command)
    
    def shutdown(self):
        """
        Stops the server and socket connections gracefully.
        """
        command = {
            "action": "shutdown"
        }
        return self.send_command(command)
    
    def start(self):
        """
        Start the database server.
        """
        command = {
            "action": "start"
        }
        return self.send_command(command)

    # --- Authentication ---
    def register_user(self, username, password, roles):
        """
        Register a new user.
        """
        command = {
            "action": "register_user",
            "params": {
                "username": username,
                "password": password,
                "roles": roles
            }
        }
        return self.send_command(command)

    def login_user(self, username, password):
        """
        Login a user.
        """
        command = {
            "action": "login_user",
            "params": {
                "username": username,
                "password": password
            }
        }
        return self.send_command(command)

    def logout_user(self, session_token):
        """
        Logout a user.
        """
        command = {
            "action": "logout_user",
            "params": {
                "session_token": session_token
            }
        }
        return self.send_command(command)

    # --- Table Management ---
    def create_table(self, table_name, columns):
        """
        Create a new table.
        """
        command = {
            "action": "create_table",
            "params": {
                "table_name": table_name,
                "columns": columns
            }
        }
        return self.send_command(command)

    def list_tables(self):
        """
        List all tables.
        """
        command = {
            "action": "list_tables"
        }
        return self.send_command(command)

    def drop_table(self, table_name):
        """
        Drop a table.
        """
        command = {
            "action": "drop_table",
            "params": {
                "table_name": table_name
            }
        }
        return self.send_command(command)

    # --- Record Operations ---
    def insert_record(self, table, record):
        """
        Insert a record into a table.
        """
        command = {
            "action": "insert",
            "params": {
                "table": table,
                "record": record
            }
        }
        return self.send_command(command)

    def query_table(self, table, filter=None):
        """
        Query a table with an optional filter.
        """
        command = {
            "action": "query",
            "params": {
                "table": table,
                "filter": filter
            }
        }
        return self.send_command(command)

    def update_record(self, table, record_id, updates):
        """
        Update a record in a table.
        """
        command = {
            "action": "update",
            "params": {
                "table": table,
                "record_id": record_id,
                "updates": updates
            }
        }
        return self.send_command(command)

    def delete_record(self, table, record_id):
        """
        Delete a record from a table.
        """
        command = {
            "action": "delete",
            "params": {
                "table": table,
                "record_id": record_id
            }
        }
        return self.send_command(command)

    # --- Stored Procedure Execution ---
    def create_procedure(self, procedure_name, procedure_code):
        """
        Create a stored procedure.
        """
        command = {
            "action": "create_procedure",
            "params": {
                "procedure_name": procedure_name,
                "procedure_code": procedure_code
            }
        }
        return self.send_command(command)

    def execute_procedure(self, procedure_name, procedure_params):
        """
        Execute a stored procedure.
        """
        command = {
            "action": "execute_procedure",
            "params": {
                "procedure_name": procedure_name,
                "procedure_params": procedure_params
            }
        }
        return self.send_command(command)
    
    # -- General Database Information --
    def get_db_info(self):
        """
        
        Retrieve general information about the database.
        """
        command = {
            "action": "get_db_info"
        }
        return self.send_command(command)

    # --- Views ---
    def query_view(self, view_name):
        """
        Query a view.
        """
        command = {
            "action": "query_view",
            "params": {
                "view_name": view_name
            }
        }
        return self.send_command(command)

    def create_view(self, view_name, query_code):
        """
        Create a new view.
        """
        command = {
            "action": "create_view",
            "params": {
                "view_name": view_name,
                "query_code": query_code
            }
        }
        return self.send_command(command)

    def drop_view(self, view_name):
        """
        Drop a view.
        """
        command = {
            "action": "drop_view",
            "params": {
                "view_name": view_name
            }
        }
        return self.send_command(command)

    def list_views(self):
        """
        List all views.
        """
        command = {
            "action": "list_views"
        }
        return self.send_command(command)

    # --- Materialized Views ---
    def query_materialized_view(self, view_name):
        """
        Query a materialized view.
        """
        command = {
            "action": "query_materialized_view",
            "params": {
                "view_name": view_name
            }
        }
        return self.send_command(command)

    def create_materialized_view(self, view_name, query_code):
        """
        Create a new materialized view.
        """
        command = {
            "action": "create_materialized_view",
            "params": {
                "view_name": view_name,
                "query_code": query_code
            }
        }
        return self.send_command(command)

    def drop_materialized_view(self, view_name):
        """
        Drop a materialized view.
        """
        command = {
            "action": "drop_materialized_view",
            "params": {
                "view_name": view_name
            }
        }
        return self.send_command(command)

    def list_materialized_views(self):
        """
        List all materialized views.
        """
        command = {
            "action": "list_materialized_views"
        }
        return self.send_command(command)

class SocketUtilities:
    @staticmethod
    def print_results(data, columns, limit=10, offset=4):
        """
        Print the results of a query in a formatted manner.

        Args:
            data (list): List of records (dictionaries) to display.
            columns (list): List of column names.
            limit (int): Maximum number of rows to display.
        """
        if not data or not columns:
            print("No data to display.")
            return
        
        try:
            from tabulate import tabulate
            
            # Convert list of dicts to list of lists based on column order
            rows = [[record.get(col, "") for col in columns] for record in data]
            
            # Apply limit to the number of rows to display
            if len(rows) > limit: rows = rows[:limit]  # Limit the number of rows to display
            
            print(tabulate(rows, headers=columns, tablefmt="rounded_outline"))
            
            if len(rows) > limit: print(f"  --Displaying {limit} of {len(data)} records--")
            return
        
        except Exception as e:
            print(f"Failed to use tabulate for formatting: {e}. Falling back to manual formatting.")
            
            # Calculate column widths
            col_widths = {col: max(len(col), max(len(str(record.get(col, ""))) for record in data)) for col in columns}

            # Create offset
            offset = " " * offset

            # Print the column headers
            header = "|" + " | ".join(col.ljust(col_widths[col]) for col in columns) + "|"
            print(offset + "|" + "-" * (len(header)-2) + "|")
            print(offset + header)
            print(offset + "|" + "-" * (len(header)-2) + "|")

            # Print the data rows
            for i, record in enumerate(data):
                if i >= limit:
                    print(offset + "|" + "-" * (len(header)-2) + "|")
                    res = f"--Displaying {limit} of {len(data)} records--"
                    print(offset + ((len(header)-2)//2 - len(res)//2) * " " + res)
                    return 
                
                row =  offset + "|" + " | ".join(str(record.get(col, "")).ljust(col_widths[col]) for col in columns) + "|"
                print(row)
                
            print(offset + "|" + "-" * (len(header)-2) + "|")