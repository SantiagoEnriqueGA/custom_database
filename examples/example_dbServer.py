import sys
import os

# Change the working directory to the parent directory to allow importing the segadb package.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from segadb import *
import time

# Create a new database
db = Database.load_sample_database(n_users=10, n_orders=10, n_products=100, 
                                   n_reviews=20, n_categories=3, n_suppliers=3,
                                   db_logging=True, table_logging=False)

host = '127.0.0.1'
port = 65432

# Start the database in a separate thread
db.start_in_thread()

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