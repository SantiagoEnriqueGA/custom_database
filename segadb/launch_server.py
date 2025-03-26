# This file launches a server from a .segadb file 
# Takes inputs from cli for file path, optional user and password, host and port
# Example usage:
# python segadb/launch_server.py example_storage/database.segadb --user admin --password password123 --host 127.0.0.1 --port 65432

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import argparse
import time
from segadb import Storage

def main():
    parser = argparse.ArgumentParser(description="Launch a server from a .segadb file.")
    parser.add_argument("file", help="Path to the .segadb file.")
    parser.add_argument("--user", help="Username for authentication.", default=None)
    parser.add_argument("--password", help="Password for authentication.", default=None)
    parser.add_argument("--host", help="Host for the socket server.", default="127.0.0.1")
    parser.add_argument("--port", help="Port for the socket server.", type=int, default=65432)

    args = parser.parse_args()

    # Load the database
    try:
        db = Storage.load(args.file, user=args.user, password=args.password)
    except Exception as e:
        print(f"Error loading database: {e}")
        return

    # Start the database in a separate thread
    db.start_db_in_thread()

    # Start the socket server
    db.start_socket_server(host=args.host, port=args.port)

    print(f"Server started on {args.host}:{args.port}. Press Ctrl+C to stop.")

    # Keep the server running
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down server...")
        db.stop_socket_server()
        db.stop()

if __name__ == "__main__":
    main()