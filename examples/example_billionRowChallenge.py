import sys
import os

# Change the working directory to the parent directory to allow importing the segadb package.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from segadb import *

# Create a new database
db = Database("BRC_DB")

# Create a new table
db.create_table_from_csv("example_datasets/measurements_s.txt", "BRC", headers=False, delim=';', column_names=['station', 'measure'])


# db.get_table("BRC").print_table()

