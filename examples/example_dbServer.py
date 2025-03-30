import sys
import os
import argparse
import time
import signal

try:
    # Adjust import relative to where run_all.py is (project root)
    from segadb import *
except ImportError:
    # Fallback if run standalone - requires parent dir in PYTHONPATH or adjusted sys.path
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
    from segadb import *

def main():

    # Create a new database
    db = Database.load_sample_database(n_users=1_000, n_orders=10_000, n_products=100, 
                                    n_reviews=200, n_categories=10, n_suppliers=10,
                                    db_logging=True, table_logging=True)

    parser = argparse.ArgumentParser(description="Run the SegaDB server.")
    parser.add_argument("--host", help="Host for the socket server.", default="127.0.0.1")
    parser.add_argument("--port", help="Port for the socket server.", type=int, default=65432)

    args = parser.parse_args()
    host = args.host
    port = args.port

    # Start the database in a separate thread
    db.start_db_in_thread()

    # Start the socket server
    db.start_socket_server(host=host, port=port)

    # Let the database run indefinitely
    try:
        while True:
            time.sleep(1)
            # pass
    except KeyboardInterrupt:
        db.stop_socket_server()
        db.stop()

if __name__ == "__main__":
    main()